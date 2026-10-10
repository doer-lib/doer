# E2E test application: Transit Sims

Status: proposed

## What it is

Transit Sims is a small Jakarta EE backend application that uses Doer the way a Doer user would: CDI beans with doer methods, loaders and savers, REST endpoints, JDBC and calls to external services. It simulates buses and passengers on a city transport network; see [transit-sims.md](transit-sims.md). It replaces CarWash, today's test application in `src/test/resources/e2e/carwash`.

It exists to test Doer and is not part of the library. It has to answer two questions about the same code (see [e2e-design.md](e2e-design.md)):

- **Does the annotation processor generate correct code?** `*GeneratedCodeITCase` compiles Transit Sims with every JDK and build tool and runs [`GeneratedCodeTest`](#generated-code-generatedcodetest) on it, without a database or a container.
- **Does the generated code work in a real runtime?** The e2e suite deploys Transit Sims to Quarkus, Spring Boot, WildFly and the others, and tests it over REST, JDBC and WireMock.

So Transit Sims has to contain **every kind of component a Doer user writes**, in every way a user writes it, and it has to stay within [the one rule](e2e-design.md#the-one-rule): only `com.doer`, Jakarta Web Profile APIs and `javax.sql`.

The cases in [What Transit Sims must cover](#what-transit-sims-must-cover) are of two kinds:

- **The generated code**: what `_GeneratedDoerService` itself does. It is checked by one set of tests, `GeneratedCodeTest`, which runs both in `*GeneratedCodeITCase` and in e2e.
- **Everything else**: transactions, locks, real dependency injection by CDI, interruption on stop, exception descriptions in `task_logs`. It is checked by the other e2e tests. In `*GeneratedCodeITCase` these cases only have to compile.

## Why Transit Sims

- **It is easy to see.** The browser draws the network and animates buses and passengers, so a broken run shows as a bus that stops or a passenger who never arrives.
- **Many concurrent calls on one instance.** Passengers board and alight while the bus is stopped, all at once, against the same bus task: a real load for coordinated updates.
- **Few tables.** The application data is two tables, `sims` and `buses`; the rest of the state is in Doer's tasks.

In the suites the test code plays the passengers over REST; the browser is not used. The task data is the simulation (`Sim`) and the bus (`Bus`).

## Two parts: functional code and specialized classes

| Part | Package | What it is |
|---|---|---|
| **Functional code** | `transitsims` | what [transit-sims.md](transit-sims.md) describes, kept clear and short. It knows nothing of the tests and uses Doer only where the simulation needs it. |
| **Specialized classes** | `transitsims.validation` | one set of classes per aspect of Doer, named after it (`GeneratedCode*`, `BeanKind*`, `Concurrency*`, …), with statuses named after the classes. They cover each case the functional code does not cover naturally, and the validation endpoints. |

The functional code is not stretched to cover a case. A case it covers on its own is listed with its functional class; every other case has a specialized class.

## Transit Sims in Doer terms

### In the suites and out of them

In step 3: the backend, which means the network calculation, the REST API, and the Sim and Bus tasks. Out of step 3:

- **the frontend**. The suites do not use it.
- **the SSE stream of events**. An in-memory SSE broadcaster reaches only the clients of its own node, but a bus task runs on any node. Events across nodes need a design of their own, for example a table of events or Postgres `LISTEN`/`NOTIFY`. Until then the clients poll `GET …/buses`.
- **walking**. Passengers walk on the client side, and the backend does not see it.

### Tables

`V4__create_transit_sims_tables.sql`:

| Table | Columns |
|---|---|
| `sims` | `id`, `task_id`, `config` (the JSON as posted), `plan` (the paths between stops, calculated), `started_at`, `paused_at`, `paused_ms`, `passengers`, `arrived` |
| `buses` | `id`, `sim_id`, `task_id`, `route_id`, `stop_index`, `direction` (+1 / −1), `arrived_at_ms`, `departed_at_ms` (simulation time), `path` (the road vertices to the next stop), `capacity`, `passengers` (ids on board) |

**Simulation time** is computed by the database: the milliseconds between `started_at` and `coalesce(paused_at, now())`, minus `paused_ms`. All nodes use the clock of the database, so a pause stops every bus at once.

### Tasks

Each simulation and each bus is a Doer task. **The bus is simulated in steps**: its doer method runs again and again, every second (the delay of its status), and leaves the status as it is until something happens.

When the bus departs, it gets the `path` to the next stop: the road vertices from the last stop to the next one, and `departed_at_ms`, the simulation time of the departure. From them, and from `speed`, a client draws the bus between steps, and a step finds out whether the bus has arrived. Both times are simulation time, so a paused bus stays where it is.

| Task | Status | Doer method | What one step does |
|---|---|---|---|
| Sim | `Sim ready` | — | waits for `start` |
| Sim | `Sim running` (delay `1s`) | `SimSupervisor.checkCompletion(Task, Sim)` | `Sim completed` when all passengers have arrived and all buses are parked. |
| Sim | `Sim paused` | — | waits for `resume` |
| Bus | `Bus at stop`, `Bus at terminal` (delay `1s`) | `BusDriver.stand(Task, Bus)` | after the dwell since `arrived_at_ms` (2 s at a stop, 5 s at a terminal): `path` and `departed_at_ms` of the next stop, `Bus driving`; the direction is reversed at a terminal. When all passengers have arrived and the bus is empty at a terminal: `Bus parked`. |
| Bus | `Bus driving` (delay `1s`) | `BusDriver.drive(Task, Bus)` | when the length of `path` is covered at `speed` since `departed_at_ms`: `arrived_at_ms`, `Bus at stop` or `Bus at terminal` |
| Bus | `Bus parked` | — | the end of the bus |

The statuses are constants of `BusStatus` and `SimStatus`.

**Task data.** `SimRepository` loads and saves `Sim`, and `BusRepository` loads and saves `Bus`, with JDBC. Neither has doer methods. The `Bus` loader joins `sims`, so a `Bus` also carries the simulation time, whether the simulation is running, and whether all passengers have arrived. These values are read-only: the `Bus` saver writes only `buses`. So the bus methods do not take `Sim`, and its saver is not called by many bus tasks at once.

### REST API

| Endpoint | Does |
|---|---|
| `POST /api/sims` | `@Transactional`: calculates the network (`Network`), inserts the `sims` row, the buses and the tasks (Sim `Sim ready`, buses `Bus at stop` / `Bus at terminal`). Returns `{"id": …}`. |
| `GET /api/sims/{sim}` | status, simulation time, passengers, arrived, stops, routes with their paths |
| `GET /api/sims/{sim}/buses` | for each bus: id, route, status, stop, next stop, `path` with the coordinates of its vertices, `departedAt`, passengers, and the simulation time |
| `POST /api/sims/{sim}/start`, `pause`, `resume` | a coordinated update of the Sim task with `Sim`: status and clock |
| `POST /api/sims/{sim}/passengers` | before the start: registers a passenger and returns its number (a coordinated update of `Sim`) |
| `POST /api/sims/{sim}/passengers/{p}/arrived` | `arrived` + 1 (a coordinated update of `Sim`) |
| `POST /api/sims/{sim}/buses/{bus}/board`, `alight` | `{"passenger": p, "stop": "s2"}`: a coordinated update of the Bus task with `Bus`. Responds 409 when the bus is not stopped at that stop, or when it is full. |

The configuration of `POST /api/sims`:

```json
{"vertices": [{"id": "a", "x": 0, "y": 0}, {"id": "b", "x": 30, "y": 0}],
 "edges": [["a", "b"]],
 "stops": [{"id": "s1", "name": "Central", "x": 0, "y": 1}, {"id": "s2", "name": "Park", "x": 30, "y": 1}],
 "routes": [{"id": "r1", "name": "1", "stops": ["s1", "s2"]}],
 "buses": 2, "capacity": 20, "speed": 10}
```

`speed` is in units of length per second. `Network` is plain Java. It calculates the edge lengths and the nearest vertex of each stop, finds the shortest paths between consecutive stops with Dijkstra, and gives each route a share of the buses proportional to its length.

### Classes

| Class | Kind |
|---|---|
| `TransitSimsApplication`, `DoerLifecycle`, `RestExceptionMapper` | `@ApplicationPath("/api")`, Doer start and stop, errors as JSON |
| `SimResource`, `BusResource` | JAX-RS resources of the API |
| `SimSupervisor` | `@Dependent`, the doer method of the Sim task |
| `BusDriver` | `@ApplicationScoped`, the doer methods of the Bus task |
| `SimRepository`, `BusRepository` | `@ApplicationScoped`, loader and saver, JDBC |
| `Sim`, `Bus`, `Network`, `SimStatus`, `BusStatus` | data, calculation, status names |

## Specialized classes

All of them are in `transitsims.validation`. The statuses carry the name of the class (`Bean kind singleton`, `Concurrency limit 1`, …), so that a test sees whose method ran.

| Aspect | Classes | Tested by |
|---|---|---|
| generated code | `GeneratedCodeMethods`, `GeneratedCodeFailures`, `GeneratedCodeTaskData`, `TaskRunner`, `CallTrace` (as today) | `GeneratedCodeTest` |
| kinds of beans | `BeanKindSingleton`, `BeanKindProduced` with `BeanKindProducers`, `BeanKindIntercepted` with the binding `@BeanKindTraced`, `BeanKindConstructor`, `BeanKinds.Nested`, `BeanKindInherited` extends `BeanKindBase` | `BeanKindsE2E` |
| doer methods | `DoerMethodStatuses` (several `@AcceptStatus`, delay, `null`, constants of `DoerMethodStatusNames`, `switch`, lambda), `DoerMethodCalls` (calls `DoerService`), long-running methods | `DoerMethodsE2E` |
| concurrency | `ConcurrencyLimits` (class and method, class → method → class), `ConcurrencyGroupFirst`, `ConcurrencyGroupSecond` (one group in two classes, on a class and on a method) | `ConcurrencyE2E` |
| transactions | `TransactionData` (loader and saver that write `demo_log_tasks` with `txid_current()`), `TransactionMethods` (`updateAndBumpVersion` in a doer method, a doer method with its own `@Transactional`) | `TransactionsE2E` |
| task data | `TaskDataRecord` (a record, a loader without a saver) | `DoerMethodsE2E` |
| errors | `ErrorMethods` (checked, runtime, `@RetryPolicy` with a fallback and without a duration, cause and suppressed; describer of `RuntimeException`), `ErrorDescribers` (describer of `Exception`, a bean of its own) | `ErrorsE2E` |
| external service | `ExternalServiceMethods` (JAX-RS client to WireMock: success, error, timeout), `ExternalServiceConfig` (URL from `E2E_EXTERNAL_URL`) | `ExternalServiceE2E` |
| validation endpoints | `ValidationResource` (`/api/validation`, today's `DoerResource`; also the doer methods of a JAX-RS resource) | all `*E2E` |

`ValidationResource.reset` also deletes `sims` and `buses`, because a test of an aspect counts all rows of `tasks` and `task_logs`.

## The e2e tests

| Today | In Transit Sims |
|---|---|
| `SmokeE2E` | unchanged; migrations V1–V4 |
| `GeneratedCodeE2E` | unchanged |
| `DoerMethodsE2E`, `ConcurrencyE2E`, `TransactionsE2E`, `ErrorsE2E` | the same checks on the specialized classes of their aspect, and new checks for the cases marked `—` |
| `CoordinatedUpdateE2E` | unchanged, on `ValidationResource` |
| — | `BeanKindsE2E`, `ExternalServiceE2E`: specialized classes |
| — | `TransitSimsE2E`: a small network. Buses go from stop to stop. Concurrent boarding respects the capacity and the stop. A pause stops the buses. The simulation completes. |
| — | `LifecycleE2E` on Transit Sims: a node stops while tasks are in progress and is restarted; two nodes run one simulation. |

## What Transit Sims must cover

`—` in the first column marks a case today's code does not cover yet. **Code** points to where today's code covers it: these are still CarWash classes, which Transit Sims has to replace without losing the case. **Transit Sims** is the class that covers it after step 3: a functional class (see [Classes](#classes)) or a specialized one.

### Generated code: `GeneratedCodeTest`

What `runTask` of the generated service does with a task. Not the annotations: the variety of annotations is the job of `GeneratorITCase`.

The doer methods, loaders and savers for these checks are in the validation package, in classes and statuses named after what they check (`Generated code 1 param`, `Generated code 2 params`, …). The functional beans know nothing of the tests. Each check is a test method of [`GeneratedCodeTest`](src/test/java/com/doer/generatedcode/GeneratedCodeTest.java).

| | Case | Code |
|---|---|---|
| | the beans are injected into the generated service (`_inject_*`), and the method of the right bean is called | `GeneratedCodeMethods`, `GeneratedCodeFailures`, `GeneratedCodeTaskData` |
| | doer method with 1 parameter: `Task` | `Generated code 1 param`: `GeneratedCodeMethods.oneParam(Task)` |
| | doer method with 2 parameters, `Task` not first; 1 loader, in a bean without doer methods | `Generated code 2 params`: `GeneratedCodeMethods.twoParams(GeneratedCodeFirstData, Task)` ← `GeneratedCodeTaskData.loadFirst` |
| | doer method with 3 parameters; 2 loaders, in different beans | `Generated code 3 params`: `GeneratedCodeMethods.threeParams(Task, GeneratedCodeFirstData, GeneratedCodeSecondData)` ← `GeneratedCodeTaskData.loadFirst`, `GeneratedCodeMethods.loadSecond` |
| | savers: called after the method with the objects the loaders returned; not called after an exception | `GeneratedCodeTaskData.saveFirst`, `GeneratedCodeMethods.saveSecond` |
| | the method throws a checked exception or a `RuntimeException`: `failingSince` is set, the status goes back to the initial one | `Generated code checked exception`, `Generated code runtime exception`: `GeneratedCodeFailures` |
| | the method still fails 1 day after `failingSince`, no `@RetryPolicy`: the status is set to `null` (and not before) | `Generated code runtime exception`: `GeneratedCodeFailures.runtimeException` |
| | the method still fails after `@RetryPolicy(duration)`: the status is set to `fallbackStatus` (and not before) | `Generated code retry policy`: `GeneratedCodeFailures.retryPolicy` |

#### The same tests in `*GeneratedCodeITCase` and in e2e

`GeneratedCodeTest` is an interface with the test methods (`@Test` default methods) and the expected results; each implementation builds or deploys the application its own way. All of them are in the package `com.doer.generatedcode`:

```
GeneratedCodeTest                     test methods, expected results; String runTask(String request)
├── JavacGeneratedCodeITCase          builds with javac in @BeforeAll, runs CarWashProcess
├── MavenGeneratedCodeITCase          builds with Maven in @BeforeAll, runs CarWashProcess
├── GradleGeneratedCodeITCase         not yet
└── GeneratedCodeE2E                  the application deployed in the runtime, over REST
```

The names end in `ITCase` and `E2E`: that is how failsafe and the `e2e` profile select the classes. The `ITCase` classes stay in the JDK matrix; `GeneratedCodeE2E` runs in each runtime.

**The runner is part of the application.** `validation.TaskRunner` takes a request, inserts the task, calls `doerService.runTask(task)` and returns a response: the final status, `failingSince` set or not, and the call trace. The trace is recorded by the `CallTrace` bean, by task id; the GeneratedCode* doer methods, loaders and savers write to it. Task data records who got it after the loader, so the trace shows that the method and the saver get the same object:

```
→ {"status": "Generated code 2 params"}
← {"status": "Generated code 2 params done", "failing": false, "trace": ["GeneratedCodeTaskData.loadFirst", "GeneratedCodeMethods.twoParams(First[loadFirst])", "GeneratedCodeTaskData.saveFirst(First[loadFirst, twoParams])"]}
→ {"status": "Generated code runtime exception", "failingFor": "PT25H"}
← {"status": null, "failing": false, "trace": ["GeneratedCodeFailures.runtimeException"]}
```

Requests and responses are JSON objects, one per line ([JSON Lines](https://jsonlines.org/)). `failingFor` is how long ago `failingSince` was, not a timestamp, so the clocks of the test, the application and the database do not have to agree. Both suites send the same requests to the same `TaskRunner`, so they expect the same responses. The test pretty-prints each response with `JsonPath.prettify` (the fields in the order TaskRunner writes them, one field or call per line) and compares it with a text block by `assertEquals`, so a failure shows the line that differs.

- **`*GeneratedCodeITCase`** adds a small `Main` (the text block `CarWashProcess.MAIN`): it wires the beans by hand, with `TestDoerService` instead of a database (its `insert` only gives the task an id), reads requests from `stdin` line by line, passes each to `TaskRunner` and writes the response as one line to `stdout`. Each class builds the application in `@BeforeAll` and starts `java -cp <built classes>:<doer jar>:<Jakarta EE API>:<Parsson> carwash.Main` once for all its test methods; each test method writes a line and reads a line; `@AfterAll` closes `stdin`, and `Main` exits. Only JSON Lines go to `stdout`; the logs go to `stderr`, kept in `main-err.txt` of the workspace of the class (`target/it-test-workspaces/<class>/`).
- **`GeneratedCodeE2E`** sends the same request as the body of `POST /api/validation/run-task`, which calls the same `TaskRunner` and returns its response. There, `runTask` goes through the CDI proxy of the generated service, with real injection and real transactions. The task must be in the database (`runTask` updates it by version), and Doer's scheduler must not take it: `TaskRunner` inserts it already in progress, which the scheduler skips, so Doer keeps running. A task left failing is retried by the scheduler later; its calls go to its own id in `CallTrace` and do not mix with the next test.

### Beans with doer methods

| | Case | Why it matters | Code | Transit Sims |
|---|---|---|---|---|
| | `@ApplicationScoped` bean | the generated service gets a client proxy | `CarWash`, `PhoneBooth` | `BusDriver` |
| | `@Dependent` bean | no proxy; one instance for the lifetime of the generated service, shared by all Doer threads | `Cafeteria` | `SimSupervisor` |
| — | `@jakarta.inject.Singleton` bean | pseudo-scope, no proxy; Spring and CDI treat it differently | | `BeanKindSingleton` |
| — | bean created by a producer (`@Produces` method or field) | the class has no bean-defining annotation; the generated service injects it by type | | `BeanKindProduced` ← `BeanKindProducers` |
| — | bean with an interceptor (an interceptor binding of Transit Sims on the class or on a doer method) | the doer method is called through the interceptor chain | | `BeanKindIntercepted`, `@BeanKindTraced` |
| — | doer method with `@Transactional` of its own | `runTask` is `NOT_SUPPORTED`; the method's own transaction is separate from Doer's status update | | `TransactionMethods` |
| — | bean with constructor injection (`@Inject` constructor) | CDI needs a no-arg constructor for the proxy as well; `Main` must still be able to build it | | `BeanKindConstructor` |
| | JAX-RS resource with doer methods | its default scope differs by runtime (singleton in Quarkus, per request elsewhere) | `DoerResource` (no scope annotation) | `ValidationResource` |
| | bean in another package | imports and field names in the generated service | `carwash.validation.DoerResource` | every specialized class (`transitsims.validation`) |
| — | public static nested class | name of the class in the generated service | | `BeanKinds.Nested` |
| — | doer methods inherited from an abstract base class | which class the generated service calls | | `BeanKindInherited` extends `BeanKindBase` |

### Doer methods

| | Case | Code | Transit Sims |
|---|---|---|---|
| | several `@AcceptStatus` on one method | `CarWash.polishTheCar`, `PhoneBooth.makeACall` | `BusDriver.stand` |
| | `@AcceptStatus` with `delay` | `Cafeteria.waitReceiptIsPrinted` | `BusDriver.stand`, `drive`, `SimSupervisor.checkCompletion` |
| | status set to `null` (end of the process) | `DoerResource.consumeTaskB` | `DoerMethodStatuses` |
| — | statuses from constants of another class, from `switch` and lambdas | | `BusStatus`, `SimStatus`; `DoerMethodStatuses` |
| — | `@RetryPolicy` without `duration` (retries forever) | | `ErrorMethods` |
| | calls `DoerService` itself (insert a new task, `updateAndBumpVersion`) | `DoerResource.washHands` | `DoerMethodCalls`, `TransactionMethods` |
| — | calls an external service (JAX-RS client to WireMock): success, error, timeout | | `ExternalServiceMethods` |
| | long-running method | `Cafeteria.recordCustomersOrder`, `PhoneBooth.makeACall` (`Thread.sleep`) | `ConcurrencyLimits` |
| | the status stays the same; the method runs again after the delay | | `BusDriver.stand`, `drive`, `SimSupervisor.checkCompletion` |

### Concurrency

| | Case | Code | Transit Sims |
|---|---|---|---|
| | `@ConcurrencyLimit` on a class and on a method of it | `Cafeteria`, `Cafeteria.payForBubbleGum` | `ConcurrencyLimits` |
| — | `@ConcurrencyGroup` on a class and on a method | | `ConcurrencyGroupFirst`, `ConcurrencyGroupSecond` |
| — | one group shared by methods of several classes | | `ConcurrencyGroupFirst`, `ConcurrencyGroupSecond` |
| | moving between domains: class → method → class | `Cafeteria.selectBubbleGum` → `payForBubbleGum` → `sayGoodBay` | `ConcurrencyLimits` |

### Task data

| | Case | Code | Transit Sims |
|---|---|---|---|
| | loader and saver in the same bean as the doer method | `CarWash.loadShampoo` | `TransactionData` |
| | loader and saver in another bean | `DoerResource.loadCar`, `storeCar`, `storeShampoo` | `SimRepository`, `BusRepository` |
| — | loader without a saver | | `TaskDataRecord` |
| | loader and saver in a bean without doer methods | `validation.GeneratedCodeTaskData` | `GeneratedCodeTaskData`, `SimRepository`, `BusRepository` |
| — | loader and saver in a repository bean (JDBC) | | `SimRepository`, `BusRepository` |
| | loader and saver join Doer's transaction | `demo_log_tasks` with `txid_current()` in `CarWash.loadShampoo`, `DoerResource` | `TransactionData` |
| — | a record as task data | | `TaskDataRecord` |
| | `facilitateCoordinatedUpdate` with and without task data, from a validation endpoint | `DoerResource.coordinatedCarUpdate`, `coordinatedUpdateInTransaction` | `ValidationResource` |
| — | `facilitateCoordinatedUpdate` from a functional endpoint | | `SimResource`, `BusResource` |

The case "through a repository interface (JDBC in e2e, in memory in `Main`)" was dropped: `Main` calls only the GeneratedCode* beans, so no in-memory implementation is needed.

### Exception describers

| | Case | Code | Transit Sims |
|---|---|---|---|
| | in a bean of its own | `ExceptionMapper` | `ErrorDescribers` |
| | in a bean with doer methods | `PhoneBooth.appendRuntimeExceptionJson` | `ErrorMethods` |
| | for a subtype (`RuntimeException`) next to one for `Exception` | `PhoneBooth.appendRuntimeExceptionJson`, `ExceptionMapper.appendExceptionJson` | `ErrorMethods`, `ErrorDescribers` |
| — | exception with a cause and with suppressed exceptions | | `ErrorMethods` |

### REST endpoints

| | Case | Code | Transit Sims |
|---|---|---|---|
| — | functional endpoint that starts a business operation (inserts a task) | | `POST /api/sims` |
| — | functional endpoint with `@Transactional` that inserts a task and writes Transit Sims data in one transaction (creating a simulation) | | `POST /api/sims` |
| — | functional endpoint that does a coordinated update, called by many clients at once for the same task (boarding and alighting) | | `BusResource` board, alight |
| | validation endpoints: control Doer, read tasks, reset data, report the runtime | `DoerResource` | `ValidationResource` |

### Application lifecycle and configuration

| | Case | Code | Transit Sims |
|---|---|---|---|
| | Doer started on `Startup`, stopped on `Shutdown` | `DoerLifecycle` | `DoerLifecycle` |
| | restart with no tasks in progress | `SmokeE2E` | `SmokeE2E` |
| — | stop with tasks in progress; restart picks them up | | `LifecycleE2E` on a running simulation |
| — | two nodes on one database | | `LifecycleE2E`: one simulation, boarding through both nodes |
| — | URLs of external services from environment variables | | `ExternalServiceConfig` |

## Decisions

These questions were open in the draft.

- **Steps of one second.** Doer's delays are whole seconds today (`DoerProcessor.parseDuration`), so every bus step is `1s`. A moving bus is drawn between steps from its `path` and `departed_at_ms`. Milliseconds in Doer may come later, as a change of the library.
- **Doer runs with the monitor.** Without the monitor, a task whose delay has passed waits for the next queue reload. So `DoerLifecycle` starts Doer with `start(true)`, as a user would. `E2eEnvironment` stops Doer (`/api/validation/stop`) right after the first start of a node, so the tests of aspects keep controlling Doer through the validation endpoints as today. A node restarted by a test (`startNode`) keeps Doer as `DoerLifecycle` started it, so the tests of restarts run on a running Doer. The tests of Transit Sims start Doer with the monitor (`/api/validation/start?m=true`, or `reset?m=true`).

- **Validation code in functional beans.** No. Validation is only in `transitsims.validation`, and the functional beans know nothing of the tests. Today's `demo_log_tasks` is written only by `TransactionData`.
- **External services.** Transit Sims calls none. The JAX-RS client, WireMock and configuration cases are covered by `ExternalServiceMethods` and `ExternalServiceConfig`.
- **`@RequestScoped` beans and JAX-RS resources outside a request.** Not in step 3. Doer threads have no active request context, so a doer method in a `@RequestScoped` bean fails at the call. The documentation should say so. `ValidationResource` stays without a scope annotation, as the JAX-RS case. If a runtime makes it per request and that breaks, it is decided in that runtime's step.
- **Qualifiers.** Not in Transit Sims. The generated service injects without qualifiers, so a bean with a qualifier, or two beans of one type, is unsatisfied or ambiguous. That is a matter for the documentation, or for a check in the processor.
- **Decorators and alternatives.** Out of scope.
