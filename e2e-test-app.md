# E2E test application: Transit Sims

Status: draft

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

## What Transit Sims must cover

`—` in the first column marks a case today's code does not cover yet. **Code** points to where today's code covers it: these are still CarWash classes, which Transit Sims has to replace without losing the case.

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

| | Case | Why it matters | Code |
|---|---|---|---|
| | `@ApplicationScoped` bean | the generated service gets a client proxy | `CarWash`, `PhoneBooth` |
| | `@Dependent` bean | no proxy; one instance for the lifetime of the generated service, shared by all Doer threads | `Cafeteria` |
| — | `@jakarta.inject.Singleton` bean | pseudo-scope, no proxy; Spring and CDI treat it differently | |
| — | bean created by a producer (`@Produces` method or field) | the class has no bean-defining annotation; the generated service injects it by type | |
| — | bean with an interceptor (an interceptor binding of Transit Sims on the class or on a doer method) | the doer method is called through the interceptor chain | |
| — | doer method with `@Transactional` of its own | `runTask` is `NOT_SUPPORTED`; the method's own transaction is separate from Doer's status update | |
| — | bean with constructor injection (`@Inject` constructor) | CDI needs a no-arg constructor for the proxy as well; `Main` must still be able to build it | |
| | JAX-RS resource with doer methods | its default scope differs by runtime (singleton in Quarkus, per request elsewhere) | `DoerResource` (no scope annotation) |
| | bean in another package | imports and field names in the generated service | `carwash.validation.DoerResource` |
| — | public static nested class | name of the class in the generated service | |
| — | doer methods inherited from an abstract base class | which class the generated service calls | |

### Doer methods

| | Case | Code |
|---|---|---|
| | several `@AcceptStatus` on one method | `CarWash.polishTheCar`, `PhoneBooth.makeACall` |
| | `@AcceptStatus` with `delay` | `Cafeteria.waitReceiptIsPrinted` |
| | status set to `null` (end of the process) | `DoerResource.consumeTaskB` |
| — | statuses from constants of another class, from `switch` and lambdas | |
| — | `@RetryPolicy` without `duration` (retries forever) | |
| | calls `DoerService` itself (insert a new task, `updateAndBumpVersion`) | `DoerResource.washHands` |
| — | calls an external service (JAX-RS client to WireMock): success, error, timeout | |
| | long-running method | `Cafeteria.recordCustomersOrder`, `PhoneBooth.makeACall` (`Thread.sleep`) |

### Concurrency

| | Case | Code |
|---|---|---|
| | `@ConcurrencyLimit` on a class and on a method of it | `Cafeteria`, `Cafeteria.payForBubbleGum` |
| — | `@ConcurrencyGroup` on a class and on a method | |
| — | one group shared by methods of several classes | |
| | moving between domains: class → method → class | `Cafeteria.selectBubbleGum` → `payForBubbleGum` → `sayGoodBay` |

### Task data

| | Case | Code |
|---|---|---|
| | loader and saver in the same bean as the doer method | `CarWash.loadShampoo` |
| | loader and saver in another bean | `DoerResource.loadCar`, `storeCar`, `storeShampoo` |
| — | loader without a saver | |
| | loader and saver in a bean without doer methods | `validation.GeneratedCodeTaskData` |
| — | loader and saver through a repository interface (JDBC in e2e, in memory in `Main`) | |
| | loader and saver join Doer's transaction | `demo_log_tasks` with `txid_current()` in `CarWash.loadShampoo`, `DoerResource` |
| — | a record as task data | |
| | `facilitateCoordinatedUpdate` with and without task data, from a validation endpoint | `DoerResource.coordinatedCarUpdate`, `coordinatedUpdateInTransaction` |
| — | `facilitateCoordinatedUpdate` from a functional endpoint | |

### Exception describers

| | Case | Code |
|---|---|---|
| | in a bean of its own | `ExceptionMapper` |
| | in a bean with doer methods | `PhoneBooth.appendRuntimeExceptionJson` |
| | for a subtype (`RuntimeException`) next to one for `Exception` | `PhoneBooth.appendRuntimeExceptionJson`, `ExceptionMapper.appendExceptionJson` |
| — | exception with a cause and with suppressed exceptions | |

### REST endpoints

| | Case | Code |
|---|---|---|
| — | functional endpoint that starts a business operation (inserts a task) | |
| — | functional endpoint with `@Transactional` that inserts a task and writes Transit Sims data in one transaction (creating a simulation) | |
| — | functional endpoint that does a coordinated update, called by many clients at once for the same task (boarding and alighting) | |
| | validation endpoints: control Doer, read tasks, reset data, report the runtime | `DoerResource` |

### Application lifecycle and configuration

| | Case | Code |
|---|---|---|
| | Doer started on `Startup`, stopped on `Shutdown` | `DoerLifecycle` |
| | restart with no tasks in progress | `SmokeE2E` |
| — | stop with tasks in progress; restart picks them up | |
| — | two nodes on one database | |
| — | URLs of external services from environment variables (`TransitSimsConfig`) | |

## Open questions

- **`@RequestScoped` beans and JAX-RS resources outside a request.** Doer threads have no active request context. A doer method in a `@RequestScoped` bean fails at the call. Should the processor reject it, should the documentation forbid it, or should Transit Sims just show that it fails? The same question applies to `DoerResource` in runtimes where JAX-RS resources are per request.
- **Qualifiers.** The generated service injects beans without qualifiers. A bean with a qualifier (or two beans of the same type) leaves the injection unsatisfied or ambiguous. Is that a case for Transit Sims, or for the documentation?
- **Decorators and alternatives**: in scope or not?
- **Validation code in functional beans.** Today the validation table `demo_log_tasks` is written by loaders of functional beans. Should validation stay in `transitsims.validation` only, with the functional beans unaware of the tests?
- **External services.** Transit Sims as described in [transit-sims.md](transit-sims.md) calls none, but the JAX-RS client, WireMock and `TransitSimsConfig` cases need one. Which service: for example, a traffic service that tells the travel time of a road edge?
