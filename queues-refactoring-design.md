# Переделка очередей задач

Status: in progress — шаги 1–6 выполнены, в шаге 7 исправлены пункты 1–4

Логика очередей распределена между `ConcurrencyDomainImpl` и `DoerService`. Домен и его подочереди (`SubQueue`) только хранят данные. `DoerService` сам заполняет их поля, считает лимиты чтения, раскладывает выборку из БД и ведёт учёт выполняющихся задач. Цель — собрать состояние очередей и алгоритмы над ним в отдельных классах, не меняя поведения.

## 1. Как работают очереди сейчас

### Модель

**Домен** — группа конкурентности с лимитом `limit`: сколько задач домена одновременно выполняется на одном узле. Домен обслуживает статусы из `@AcceptStatus` своих doer-методов. У каждого статуса есть задержка `delay` (по умолчанию 0) и интервал повтора `retryDelay` (по умолчанию 5 мин).

Внутри домена задачи разложены по **подочередям**. Каждая подочередь соответствует одной выборке `SELECT ... LIMIT ?` в `SelectTasks.sql`:

| Подочередь | Какие задачи | Порядок в буфере | Когда задача готова |
|---|---|---|---|
| asap (delay = 0) | `failing_since IS NULL`, статус без задержки | `created, id` | сразу (`Instant.MIN`) |
| delayed, по одной на каждую задержку | `failing_since IS NULL`, статус с этой задержкой | `modified, created, id` | `modified + delay` |
| retry, по одной на каждый интервал | `failing_since IS NOT NULL`, статус с этим интервалом | `modified, created, id` | `modified + retryDelay` |

Состояние подочереди:
- `buffer` — окно в начало этой очереди в БД, то есть первые N задач в её порядке;
- `queueSize` — это N: ёмкость буфера и `LIMIT` для следующего чтения из БД;
- `hasMoreInDb` — в БД есть задачи этой очереди, которых нет в буфере.

Подочередь **исчерпана**, если `buffer` пуст, а `hasMoreInDb == true`. Это сигнал, что пора перечитать очереди.

Вся работа с очередями идёт под монитором `DoerService` (`synchronized (this)`).

### Путь задачи: очередь → inProgress → очередь

```
 buffer подочереди ──pullNextReadyTask──▶ inProgressTasks ──runTask──▶ putTaskToQueue ──▶ buffer подочереди
        ▲                                  (+1 к счётчику домена)        (домен нового статуса)       (или выпадает:
        │                                                                                              финальный статус)
        └──── reloadTasksFromDb / reloadQueuedTask (чтение из БД) ◀── если runTask бросил исключение
```

1. **Выбор задачи** — `processNextTask(domain)`, под монитором:
   - нет свободного слота (`limit - inProgress < 1`) → запускается `monitorIdleState`, выход;
   - `pullNextReadyTask(now)` берёт **готовые** головы всех подочередей домена и выбирает самую раннюю по `created, id`. Отложенная задача, которая стала готовой, конкурирует с asap-задачами по времени создания;
   - если после изъятия какая-то подочередь домена исчерпана → `triggerQueuesReloadFromDb()`;
   - задача добавляется в `inProgressTasks`, счётчик домена `numberOfTasksInProgress++`, вызывается `notify()` для монитора.
2. **Выполнение** — вне монитора, `runTask(task)`. Сгенерированный код вызывает `callDoerMethod`, а он сам пишет в БД:
   - `in_progress=true`, затем загрузчик данных, затем doer-метод;
   - при успехе: новый статус, `failing_since=null`;
   - при ошибке doer-метода: прежний статус и `failing_since=now`, либо `onErrorStatus` после `errorTimeout`.

   Ошибка doer-метода **не** выходит из `runTask` наружу. Для планировщика это «успешная обработка», и задача вернётся в retry-подочередь.
3. **Возврат** — в `finally`, под монитором: `numberOfTasksInProgress--`, задача удаляется из `inProgressTasks`. Дальше три варианта:
   - **`runTask` завершился нормально.** Ищется домен по новому статусу, вызывается `putTaskToQueue(task)`. Подочередь выбирается по признаку failing и задержке статуса. `modified` к этому моменту уже обновлён из БД, поэтому отсчёт задержки начинается заново.
     - Если у подочереди `hasMoreInDb` и задача не раньше конца буфера → задача в память не берётся: она в БД и подтянется при перечитывании.
     - Если буфер переполнен → вытесняется последняя задача, ставится `hasMoreInDb = true`.
     - Если статус финальный (ни один домен его не принимает) → задача уходит из памяти.

     Затем запускается `processNextTask` на свободные слоты нового и старого домена. Если ничего не запустилось и `inProgressTasks` пуст → монитор простоя.
   - **`TaskVersionConflictException`.** Задачу изменил другой узел или `facilitateCoordinatedUpdate` → `triggerTaskReloadFromDb(id)`.
   - **Другое исключение** (ошибка БД и т.п.). Задача в очередь не возвращается, она вернётся только через перечитывание из БД. Если в БД осталось `in_progress=true`, она ждёт сброса зависших задач (stalled reset, через 2 ч). Запускается ровно один `processNextTask(domain)`, чтобы не разгонять поток ошибок.

### Другие пути в очередь

- **`reloadQueuedTask(id)`** — перечитать одну задачу. Вызывается из `facilitateCoordinatedUpdate`, после конфликта версий и из re-check. Задача читается из БД. Если она не `in_progress` и не выполняется на этом узле: удаляется из всех буферов, кладётся через `putTaskToQueue` в домен по статусу, затем `notify()` монитору и `processNextTask`, если есть слот. Если ждёт полное перечитывание (`taskLoadRequired`), вызов пропускается.
- **re-check** (`reCheckTasks`). Сюда попадают задачи, которые при полном перечитывании оказались в БД `in_progress` (на другом узле) или старее версии в памяти. `checkTasksBecomeReady` раз в ≥2 с перечитывает их по одной:
  - задача удалена → убрать из списка;
  - версия выросла → `triggerTaskReloadFromDb`;
  - `modified` старше 10 с → убрать из списка без перечитывания.

### Полное перечитывание из БД

Кто вызывает `triggerQueuesReloadFromDb()`:
- `start()`;
- исчерпанная подочередь (в `processNextTask`);
- монитор, когда с последнего чтения прошло `queueReloadInterval` (10 мин);
- `SQLException` в `reloadQueuedTask`.

Флаги `taskLoadRequired`/`loadingInProgress` гарантируют, что одновременно идёт одно чтение. Запрос, пришедший во время чтения, вызывает ещё одно чтение сразу после текущего.

`reloadTasksFromDb()`:
1. Под монитором: `limits = calculateLimits()` — по одному числу на каждую подочередь всех доменов. Также очищается re-check и запоминается `lastSelectTime`.
2. Вне монитора: один запрос `SelectTasks.sql`. Это `UNION ALL` из выборок по каждой подочереди с `LIMIT ?` плюс все строки `in_progress`.
3. Под монитором: `updateQueues(limits, tasks)`:
   - `calculateActualLoadedCounts`: сколько строк пришло для каждой подочереди (`loaded`);
   - фильтр строк: если в буфере версия новее → в re-check; если задача выполняется на этом узле → пропустить; если в БД `in_progress` → в re-check; остальные идут в буферы;
   - буфер каждой подочереди **заменяется целиком** на её задачи из выборки;
   - `hasMoreInDb = loaded >= limit`;
   - `queueSize = loaded < limit/2 ? max(limit/2, min) : limit`.
4. В конце: либо следующее чтение, если пришёл новый запрос, либо `checkTasksBecomeReady()`.

### Когда подстраиваются лимиты чтения

Лимиты меняются в двух местах.

**До чтения** — `calculateLimits()`. Учитывает две вещи:
- `drained`: есть ли исчерпанная подочередь. Берётся первая по порядку среди всех доменов;
- `used80`: с последнего чтения прошло ≥ 80% `queueReloadInterval`.

Правила:
- **Уменьшение вдвое**: если `(used80 || drained)`, `queueSize > min` и буфер заполнен больше чем наполовину → `max(queueSize/2, min)`. Такую подочередь за цикл не разобрали, и она держит лишнее.
- **Удвоение**: если `!used80 && drained` и сумма всех лимитов плюс `queueSize` исчерпанной подочереди ≤ `maxAllQueuesSize` (10000) → её лимит удваивается. За одно чтение удваивается только одна подочередь.

**После чтения** — в `updateQueues`:
- пришло меньше половины лимита → `queueSize` уменьшается до `max(limit/2, min)`;
- пришло ≥ лимита → `hasMoreInDb`.

Так подочередь, которая быстро опустошается, растёт: опустела → перечитали раньше 80% интервала → удвоили. Перечитывание по таймеру ужимает очереди, которые не разбирают.

### Неявные контракты

1. **Порядок подочередей = порядок `LIMIT ?` в SQL.** Домены идут по имени: и генератор, и вызовы `setupConcurrencyDomain` используют `TreeMap`. Внутри домена: asap, затем delayed по возрастанию, затем retry по возрастанию (`TreeSet<Duration>` в runtime, `TreeMap<Duration>` в генераторе).
2. **Интервал повтора по умолчанию в 5 мин задан дважды:** `DoerProcessor.DEFAULT_RETRY_INTERVAL` и `ConcurrencyDomainImpl.DEFAULT_RETRY_DELAY`. Генератор не передаёт статусы с интервалом по умолчанию, рассчитывая, что значения совпадают.
3. **Нулевая задержка в `@AcceptStatus` запрещена генератором.** Иначе появилась бы delayed-подочередь с той же задержкой, что у asap, и число подочередей разошлось бы с числом `LIMIT ?`.

## 2. Принятые решения

- Логика, общая для всех доменов, переезжает в новый класс `TaskQueues`.
- Учёт выполняющихся задач (`inProgressTasks` и счётчики доменов) переходит в `TaskQueues`.
- Интерфейс `ConcurrencyDomain` удаляется, `ConcurrencyDomainImpl` переименовывается в `ConcurrencyDomain`.
- `Task.isFailing()` не добавляется: признак проверяется только в `SubQueue`.
- Рефакторинг сохраняет поведение один в один, включая подозрительные места. Они исправляются отдельно (шаг 7).
- Контракт с SQL защищают тест и javadoc.
- Новые тесты пишутся по новому API. Тестов, фиксирующих поведение до рефакторинга, нет.
- Генератор передаёт интервалы повтора для всех статусов.

## 3. Целевая структура

```
DoerService                 потоки, монитор, чтение из БД, re-check
 └─ TaskQueues              всё состояние очередей в памяти узла
     ├─ ConcurrencyDomain   (бывший ConcurrencyDomainImpl)
     │   └─ SubQueue        (отдельный файл, с поведением)
     └─ inProgress          задачи, выполняющиеся на этом узле
```

- `TaskQueues`, `ConcurrencyDomain` и `SubQueue` не потокобезопасны. С ними работают только под монитором `DoerService`, это записано в javadoc каждого класса.
- Публичный API `DoerService` не меняется. `setupConcurrencyDomain(name, limit, delays, retryDelays)` сохраняет сигнатуру, потому что её вызывает генератор.
- Сообщения из `TaskQueues` пишутся через `DoerService.LOG`, чтобы имя логгера и тексты не изменились.

Набросок API `TaskQueues`:

```java
ConcurrencyDomain addDomain(String name, int limit, Map<String, Duration> delays,
                            Map<String, Duration> retryDelays, int minQueueSize);
ConcurrencyDomain domainOf(String status);                 // бывший selectConcurrencyDomain
Set<String> activeStatuses();                              // кэш переезжает сюда
Task startNext(ConcurrencyDomain d, Instant now);          // слот + pullNextReadyTask + отметка in progress; null = нечего запускать
void finish(Task task, ConcurrencyDomain d);               // счётчик--, снять отметку in progress
ConcurrencyDomain requeue(Task task);                      // putTaskToQueue в домен по статусу; null = финальный статус
ConcurrencyDomain replaceTask(Task fromDb);                // для reloadQueuedTask: убрать из всех буферов и положить заново
boolean isInProgress(long id);  boolean hasTasksInProgress();
List<Integer> nextLimits(Instant now, Instant lastSelect, Duration queueReloadInterval,
        int minSingleQueueSize, int maxAllQueuesSize);                                  // бывший calculateLimits
List<Task> replaceFromDb(List<Integer> limits, List<Task> rows, int minQueueSize);   // бывший updateQueues; возвращает задачи для re-check
Instant earliestReadyTime();                               // для getNextCheckTime: null = пусто, Instant.MIN = есть готовая asap-задача
List<Integer> volumes();
```

Параметры чтения `queueReloadInterval`, `minSingleQueueSize`, `maxAllQueuesSize` передаются в `nextLimits` по отдельности (record `LoadSettings`, который их объединял, убран: ясности он не добавлял). Поля и сеттеры остаются в `DoerService` и передаются при каждом вызове. Поэтому `setMinSingleQueueSize` после `setupConcurrencyDomain` по-прежнему не меняет начальный размер уже созданных подочередей.

## 4. Шаги

Шаги 1–4 — чистый рефакторинг. Каждый шаг — отдельный коммит, после каждого `mvn test`, IT и e2e. Шаги 5–6 не зависят от остальных. Шаг 7 — только после 1–4.

Тестов, фиксирующих поведение до рефакторинга, нет. На шагах 1–3 изменение поведения поймают только существующие тесты `DoerServiceTest`, `DoerServiceJdbcITCase` и e2e. Поэтому ожидаемые значения в новых тестах выводятся из текущих формул, а в описании каждого коммита перечисляется, почему поведение не меняется.

### Шаг 1. `SubQueue` с поведением

Файл `SubQueue.java`. Поля `private`: `failing`, `delay`, `statuses` (Set), `buffer`, `capacity` (бывший `queueSize`, в javadoc: «ёмкость буфера и LIMIT следующего чтения»), `hasMoreInDb`. Компараторы `ASAP_ORDER`/`DELAYED_ORDER` переезжают сюда и становятся `static final`.

| Метод | Откуда | Примечание |
|---|---|---|
| `accepts(task)` | проверки в `putTaskToQueue` и `takeTasksForQueue` | `(failingSince != null) == failing && statuses.contains(status)` |
| `offer(task)` | `putTaskToQueue` | без лишней ветки `else hasMoreInDb = true` |
| `peek` / `poll` / `remove(id)` | `pullNextReadyTask`, `dropTask` | |
| `readyTime(task)`, `isHeadReady(now)` | `getTaskReadyTime` | `!failing && delay.isZero() ? MIN : modified + delay` |
| `isDrained()` | `hasDrainedQueue`, `calculateLimits` | |
| `nextLimit(shrink, min)` | тело цикла в `calculateLimits` | |
| `replace(tasks, limit, loaded, min)` | тело цикла в `updateQueues` | |

Почему поведение не меняется: статусы подочереди — это ровно те статусы, у которых её признак failing и её задержка. Значит, `accepts` совпадает со старым сравнением `delay.equals(getDelay(status))`, а `readyTime` — со старым `getTaskReadyTime`.

`DoerService` переходит с полей на эти методы, логика пока остаётся в нём.

**Тесты** `SubQueueTest`:
- `offer`: в пределах ёмкости; переполнение вытесняет последнюю задачу и ставит `hasMoreInDb`; при `hasMoreInDb` задача берётся, только если она раньше конца буфера; при пустом буфере и `hasMoreInDb` задача не берётся;
- `readyTime` для трёх типов подочередей;
- `nextLimit`: граница `capacity/2`, нижний предел `min`;
- `replace`: `loaded < limit/2`, `loaded == limit`.

### Шаг 2. `ConcurrencyDomain` — один класс с конструктором

- Удалить интерфейс, переименовать `ConcurrencyDomainImpl` в `ConcurrencyDomain`, все поля `private final`, кроме счётчика.
- Конструктор строит подочереди: `groupStatuses(delay)` и `groupStatuses(retryDelay)` через `TreeMap<Duration, Set<String>>`. Порядок прежний: asap, delayed по возрастанию, retry по возрастанию. Он описан в javadoc как контракт с `SelectTasks.sql`.
- Мапы `delays`/`retryDelays` после конструктора не нужны и не хранятся. `getDelay`/`getRetryDelay` уходят: их заменили `SubQueue.accepts` и `readyTime`.
- `statuses` — отсортированный неизменяемый Set, метод `accepts(String status)`. У завершённой задачи статус `null`, и `accepts(null)` должен возвращать `false`, как раньше `List.contains(null)`. Поэтому `TreeSet.contains` вызывается только для не-null, а в `SubQueue` используется `HashSet`, а не `Set.copyOf`: у обоих `contains(null)` бросает NPE.
- `pullNextReadyTask`: поиск минимума среди готовых голов по `ASAP_ORDER` со строгим `<`. При равенстве выигрывает первая подочередь, как было.
- Счётчик: `hasFreeSlot()`, `taskStarted()`, `taskFinished()`, а также `limit()` и `inProgressCount()`. Последние два нужны только чтобы дословно сохранить выражение `nNew` (см. шаг 7).

**Тесты** `ConcurrencyDomainTest`:
- набор и порядок подочередей для конфигурации из `DoerServiceTest.start__should_reloadTasksFromDb_and_start_processing_loaded_tasks`;
- выбор между asap и ставшей готовой отложенной задачей по `created`;
- голова, которая ещё не готова, пропускается;
- при равенстве берётся первая подочередь.

### Шаг 3. `TaskQueues`

Переносятся из `DoerService`:
- `domains` и `statusesCache`;
- `selectConcurrencyDomain` → `domainOf`;
- `inProgressTasks` → `Map<Long, Task>` по id;
- `calculateLimits` → `nextLimits`;
- `updateQueues` + `calculateActualLoadedCounts` + `groupStatusesByDelay` + `takeTasksForQueue` → `replaceFromDb`;
- `getInMemoryTasks`, `getQueueVolumes`, `createLimitVolumeMessage`;
- обход подочередей из `getNextCheckTime` → `earliestReadyTime`.

Что меняется по сути и почему это безопасно:
- **Одна раскладка вместо двух.** Каждая строка `rows` попадает в первую подочередь, которая её принимает, порядок строк сохраняется. `loaded` = размер списка подочереди, новый буфер = этот список после фильтра (newer / in progress здесь / in progress в БД). «Разложить, потом отфильтровать» даёт то же, что «отфильтровать, потом разложить». In_progress-строки по-прежнему входят в `loaded`: это сохраняется намеренно, исправление в шаге 7.
- **Позиционное сопоставление.** `limits.get(i)` в цикле по одному плоскому списку `queues` вместо двух `LinkedList.pollFirst()`.
- **`inProgressTasks` как `Map` по id.** Дубликатов id в нём не бывает (`reloadQueuedTask` и `updateQueues` их не допускают), поэтому удаление по id равносильно удалению по ссылке.
- **`addDomain` под монитором.** Сейчас `domains.add` вызывается вне `synchronized`, поэтому там `ConcurrentLinkedQueue`. В `TaskQueues` обычный `ArrayList`, и `setupConcurrencyDomain` целиком переходит под монитор. Это вызывается при инициализации, до `start`.
- **`reCheckTasks` остаётся в `DoerService`**, потому что для re-check нужен ввод-вывод вне монитора. `replaceFromDb` возвращает список, а `DoerService` добавляет его в `reCheckTasks`.

**Тесты** `TaskQueuesTest`:
- удвоение исчерпанной подочереди и потолок `maxAllQueuesSize`;
- удваивается только первая исчерпанная;
- уменьшение вдвое при `used80` и при `drained`;
- `loaded < limit/2`;
- классификация строк в `replaceFromDb`: newer → re-check, in progress здесь → пропуск, in progress в БД → re-check;
- `earliestReadyTime`.

### Шаг 4. Упрощение мест вызова в `DoerService`

- `processNextTask`: два одинаковых выхода «нет слота / нет задачи → монитор» сводятся к `task = queues.startNext(...); if (task == null) {...}`. В `finally`: `queues.finish(...)`, затем `queues.requeue(task)`. Выражение `nNew` сохраняется дословно, с `// TODO` со ссылкой на шаг 7.
- `reloadQueuedTask` → `queues.isInProgress(id)` и `queues.replaceTask(task)`.
- `checkTasksBecomeReady` → `domain.hasFreeSlot()`.
- `getNextCheckTime`: `TreeSet` заменяется переменной `earliest`, недостижимая ветка `plannedTimes.isEmpty()` удаляется.
- Удалить `import ...ConcurrencyDomainImpl.SubQueue`. Поля `inProgressTasks`, `reCheckTasks`, `lastReCheck` сделать `private`.

### Шаг 5. Тест контракта с SQL и javadoc

- Сделано отдельным классом `SelectTasksOrderITCase`, а не в `DoerServiceJdbcITCase`. Для проверки порядка нужны домены с несколькими задержками и интервалами повтора, а исходники Orders/Payments из `DoerServiceJdbcITCase` завязаны на его SQL-тесты. Postgres новому тесту не нужен.
- Тест компилирует два класса: Billing (задержки 90s и 2m, интервалы 20s и 1m — их порядок как строк и как длительностей различается) и Shipping (интервалы 2m и 10m и интервал по умолчанию 5m между ними). Затем разбирает выборки `SelectTasks.sql` (`failing_since IS [NOT] NULL`, `status IN (...)`, `ORDER BY`) и сравнивает их по порядку с подочередями сгенерированного сервиса.
- Сгенерированный сервис загружается из `classes` через `URLClassLoader`, а классы `com.doer` берутся из текущей сборки через родительский загрузчик. Для теста добавлены package-private `DoerService.queues()`, `TaskQueues.queues()`, `SubQueue.isFailing()/statuses()/isAsap()`.
- Мутационная проверка: если отсортировать задержки в `ConcurrencyDomain` по убыванию, тест падает.
- Javadoc с описанием контракта появится в двух местах: у конструктора `ConcurrencyDomain` и у `DoerProcessor.generateSelectTaskSql`, со ссылками друг на друга.

### Шаг 6. Генератор передаёт все интервалы повтора

- Убрать `retryDelays.remove(DEFAULT_RETRY_INTERVAL)` в `DoerProcessor.generateInitializeDomains`.
- Значение по умолчанию в runtime остаётся запасным для ручных вызовов `setupConcurrencyDomain` (так делают тесты). В javadoc будет сказано, что сгенерированный код на него не полагается.
- В `GeneratorITCase` добавить проверку: для метода без `@RetryPolicy` появляется `retryDelays.put("…", Duration.ofMinutes(5))`. Затем прогнать e2e: изменится только сгенерированный код, поведение нет.
- Раньше генератор пропускал и явное `@RetryPolicy(interval = "5m")`, потому что сравнивал с константой, а не проверял наличие аннотации. Теперь записываются все статусы, и это различие больше не важно. `SelectTasksOrderITCase` проверяет, что интервал 5 мин оказывается в нужной позиции между 2m и 10m.

### Шаг 7. Отдельно, после рефакторинга: исправления ошибок

Каждое исправление — отдельный коммит с тестом, который сначала падает:
1. **In_progress-строки входят в `loaded`.** Пример: подочередь с `limit = 2`, в БД 1 готовая задача и 2 in_progress с тем же статусом. Сейчас получится `hasMoreInDb = true`, после выборки единственной задачи подочередь исчерпана, и начинается повторное чтение. Исправление: в `replaceFromDb` считать `loaded` без строк `in_progress`.

   *Исправлено.* `TaskQueues.rowsByQueue` пропускает in_progress-строки: они приходят из последней выборки, у которой нет LIMIT. В re-check они по-прежнему попадают. Тест `TaskQueuesTest.replaceFromDb__should_not_count_rows_in_progress_as_loaded_rows_of_queue` — пример выше; до исправления падал.
2. **`nNew`** в `processNextTask` считается как лимит нового домена минус счётчик старого → `newDomain.freeSlots()`.

   *Исправлено.* Последствие ошибки: когда старый домен занят, задача, перешедшая в другой домен, ждёт, пока в старом освободится слот, хотя в новом слоты свободны. Тест `DoerServiceTest.processNextTask__should_start_task_moved_to_other_domain_while_its_old_domain_is_busy` работает с настоящим пулом потоков. В D1 (лимит 3) две задачи держатся в работе, третья переходит в D2, и тест ждёт её запуска в D2. До исправления падал 3 раза из 3, после проходит 5 из 5. `ConcurrencyDomain.limit()` после исправления не нужен и удалён.
3. **Задача после исключения из `runTask`** (не конфликт версий) возвращается только через полное чтение или сброс зависших задач. Сначала решить, ошибка это или задуманное поведение. В план пока не входит.

   *Решено: ошибка пользовательского кода записывается как ошибка doer-метода.* Что было:
   - исключение loader'а откатывало первую транзакцию и уходило из `runTask` наружу. Задача оставалась без `failing_since` и после каждого перечитывания сразу снова попадала в asap-очередь;
   - исключение saver'а откатывало транзакцию успеха, но объект задачи в памяти оставался изменённым (`failing_since = null`, поднятая версия). Транзакция ошибки получала `TaskVersionConflictException`, и задача висела с `in_progress = true` до сброса зависших задач;
   - исключение `createExtraJson` откатывало транзакцию ошибки, и задача так же висела с `in_progress = true`.

   Что сделано в `DoerService.callDoerMethod`:
   - ошибка loader'а, doer-метода или saver'а записывается как ошибка doer-метода: прежний статус (или `onErrorStatus` после `errorTimeout`), `failing_since`, строка task_logs с типом исключения. Перед каждой транзакцией сохраняется копия задачи, после отката она восстанавливается;
   - ошибку describer'а перехватывает и логирует сам сгенерированный `createExtraJson`: его `catch` теперь ловит те же ошибки, что `callDoerMethod`, а не только `Exception`. Ошибка doer-метода всё равно записывается;
   - уровни лога: loader, saver и describer в `createExtraJson` — SEVERE (ERROR), doer-метод — WARNING, как было;
   - ошибки пользовательского кода — multi-catch в `callDoerMethod` и в сгенерированном `createExtraJson`: `Exception` (с `InterruptedException`), `LinkageError`, `ServiceConfigurationError`, `OutOfMemoryError`, `StackOverflowError`, `AssertionError`. Остальные `Error` пробрасываются, как раньше. `TaskVersionConflictException` пробрасывается до общего `catch`;
   - после `InterruptedException` флаг прерывания восстанавливается, когда ошибка уже записана;
   - `createExtraJson(Task, Exception)` → `createExtraJson(Task, Throwable)`, в генераторе тоже;
   - `catch` стоит вокруг всей транзакции, поэтому исключение кода самого Doer в ней (например, `SQLException` из `updateAndBumpVersion` или `writeTaskLog`) тоже записывается как ошибка loader'а или saver'а. Для транзакции saver'а так было и раньше; для транзакции loader'а раньше такое исключение пробрасывалось.

   Тесты:
   - `CallDoerMethodTest` (21 тест, таблицы подделаны в памяти, транзакции с откатом);
   - четыре проверки в `GeneratedCodeTest` (loader, saver, saver после суток ошибок, describer). Они работают в `*GeneratedCodeITCase` и в `GeneratedCodeE2E`, где транзакции настоящие.

   На коде до исправления падали тесты `CallDoerMethodTest`: три теста saver'а с `TaskVersionConflict`, loader и восстановление прерывания (тест `createExtraJson`, который тогда тоже падал, удалён вместе с перехватом в `DoerService`).
4. **Узлы кластера «догоняют» задачу друг друга.** После полного чтения у обоих узлов в очередях одни и те же задачи. Проигравший при старте получал `TaskVersionConflictException` и сразу перечитывал задачу. Если победитель к этому моменту уже закончил шаг, проигравший ставил задачу себе в очередь, хотя победитель держит её у себя, и на следующем шаге они снова сталкивались. В модели на двух узлах (`ClusterTest`) получалось 6 конфликтов и 6 перечитываний на 5 задач по 3 шага.

   *Исправлено.* Правило: задачу ставит в очередь тот, кто её изменил, то есть узел, выполнивший шаг, или узел, где вызван `facilitateCoordinatedUpdate`.
   - **a.** При конфликте версий, при старте или в конце, задача отбрасывается и не перечитывается.
   - **b.** Если этот узел просили перечитать задачу, пока она у него в работе (`reloadQueuedTask` или строка «Skipped In progress» в полном чтении), запрос запоминается (`TaskQueues.reloadWhenFinished`), и задача перечитывается, когда закончится. Без этого в одноузловой гонке задача терялась до следующего полного чтения: `facilitateCoordinatedUpdate` менял задачу, которую узел уже взял из очереди, и запрос на перечитывание пропускался из-за того, что задача в работе, а старт затем получал конфликт.
   - Re-check не изменён: после полного чтения узел по-прежнему забирает себе задачи, шаг которых закончил другой узел.

   Тесты: `ClusterTest` — два узла на одной БД в памяти: каждый шаг выполняется один раз, не больше одного конфликта на задачу, без перечитываний. Отдельно одноузловая гонка с `facilitateCoordinatedUpdate`: без пункта b задача не доходит до конца. В `TaskQueuesTest` добавлены 2 теста на `reloadWhenFinished`.
