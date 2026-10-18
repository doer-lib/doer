# Transit Sims

Transit Sims is the test application of Doer: a small Jakarta EE backend that uses Doer the way a Doer user would, with CDI beans, doer methods, loaders and savers, REST endpoints, JDBC and a call of an external service. It simulates buses and passengers on a city transport network; [transit-sims.md](transit-sims.md) is the original idea, with a browser frontend.

The test suites build it with every JDK and build tool and deploy it to every supported runtime; see [src/test/README.md](../../../README.md).

## The one rule

**Transit Sims imports only `com.doer`, the Jakarta EE Web Profile APIs (CDI, JTA, JAX-RS, JSON-P, JSON-B) and `javax.sql`.** Nothing of Quarkus, Spring, WildFly or Flyway. So the same sources compile in every build tool and run in every runtime. What a runtime needs beyond that (a `DataSource` bean, an `Executor` bean, a migration, a main class) is written by the test suite of that runtime into its work folder.

Every component is also constructible without a container: `*GeneratedCodeITCase` wires the beans by hand. The classes of `transitsims.validation` get their collaborators through `@Inject` setters or a public constructor.

## Two parts

| Part | Package | What it is |
|---|---|---|
| **The simulation** | `transitsims` | the application itself, kept clear and short. It knows nothing of the tests and uses Doer only where the simulation needs it. |
| **Validation classes** | `transitsims.validation` | one set of classes per aspect of Doer (`GeneratedCode*`, `BeanKind*`, `Concurrency*`, …), with statuses named after the class (`Bean kind singleton`, `Concurrency limit 1`, …), so that a test sees whose method ran. They cover what the simulation does not, and serve the validation endpoints. |

The simulation is not stretched to cover a case: a case it covers naturally is listed with its class in [What it covers](#what-it-covers); every other case has a validation class.

## The simulation

A simulation is a road network with stops, routes and buses. Buses drive their routes back and forth; passengers board and alight while a bus stands at a stop; the simulation completes when all passengers have arrived and all buses are parked. The test code plays the passengers over REST. Out of the application: the frontend, the stream of events (an SSE broadcaster reaches only the clients of its own node, while a bus task runs on any node) and walking.

### Data and tables

`Simulation` (the network, `status`: `READY`, `RUNNING`, `PAUSED`, `COMPLETED`, and the counts of passengers) and `Bus` (`state`: `STANDING` at its stop, `DRIVING` to the next one with the `path` of road vertices, or `PARKED`; passengers, capacity) are JSON-B classes. `Simulation` also calculates: the nearest vertex of a stop, the shortest path between stops (Dijkstra), path lengths and the share of buses of each route.

Each is a row of `sims` or `buses` (`V4__create_transit_sims_tables.sql`): `id`, `task_id`, `created`, `modified`, and `json_data`, the object written by JSON-B as `JSON` (not `JSONB`, so the text stays readable).

**Simulation time** is the milliseconds since `startedAt`, without the pauses. "Now" is the time of the database (the loaders read `now()`, the endpoints `DoerService.getDbNow()`), so all nodes share one clock.

### Tasks

Each simulation and each bus is a Doer task. A task has a status only while Doer has to run it; a new, paused, parked or completed one has the status `null`.

| Task | Status | Doer method | What one run does |
|---|---|---|---|
| Sim | `Sim running` (delay `1s`) | `SimSupervisor.checkCompletion` | when all passengers have arrived and all buses are parked: `COMPLETED`, status `null` |
| Sim | `Sim paused` | — | waits for `resume` |
| Bus | `Bus resume` | `BusDriver.resume` | after a start or a resume: `Bus on route`, or `null` for a parked bus |
| Bus | `Bus on route` (delay `1s`) | `BusDriver.step` | by `Bus.state`: departs after the dwell, arrives when the `path` is covered at `speed`, reverses at a terminal, parks when it is empty and all passengers have arrived |

A bus is simulated in steps of one second (Doer's delays are whole seconds); between steps a client draws it from its `path` and `departedAt`.

**Start, pause and resume are coordinated updates of the Sim task** (`SimResource`). In the transaction of `facilitateCoordinatedUpdate` the bus tasks get their statuses with `DoerService.loadTasks` and `updateAndBumpVersion`: `Bus resume` on start and resume, `null` on pause, hijacking a bus step in progress. If a bus task changed meanwhile, its version did too, the transaction rolls back, and the endpoint responds 409. After the commit the endpoint asks Doer to reload its queues.

### REST API

| Endpoint | Does |
|---|---|
| `POST /api/sims` | creates a simulation from JSON (below): `SimRepository.create`, `@Transactional`, inserts the simulation, its buses and their tasks in one transaction |
| `GET /api/sims/{sim}` | simulation time and the `Simulation` |
| `GET /api/sims/{sim}/buses` | each bus with the status of its task, its stop and the `Bus` |
| `POST /api/sims/{sim}/start`, `pause`, `resume` | a coordinated update of the Sim task and the bus tasks (above) |
| `POST /api/sims/{sim}/passengers` | before the start: registers a passenger, returns its number |
| `POST /api/sims/{sim}/passengers/{p}/arrived` | counts an arrived passenger |
| `POST /api/sims/{sim}/buses/{bus}/board`, `alight` | `{"passenger": p, "stop": "s2"}`: a coordinated update of the Bus task; 409 when the bus does not stand at that stop or is full |

```json
{
    "road": {
        "vertices": [{"id": "a", "x": 0, "y": 0}, {"id": "b", "x": 30, "y": 0}],
        "edges": [{"from": "a", "to": "b"}]
    },
    "stops": [{"id": "s1", "name": "Central", "x": 0, "y": 1}, {"id": "s2", "name": "Park", "x": 30, "y": 1}],
    "routes": [{"id": "r1", "name": "1", "stops": ["s1", "s2"]}],
    "buses": 2, "capacity": 20, "speed": 10, "stopDwell": 2, "terminalDwell": 5
}
```

### Classes

| Class | Kind |
|---|---|
| `TransitSimsApplication`, `RestExceptionMapper` | `@ApplicationPath("/api")`, errors as JSON |
| `DoerLifecycle` | starts Doer with the monitor on `Startup`, stops it on `Shutdown` |
| `SimResource`, `BusResource` | JAX-RS resources of the API, without a scope |
| `SimSupervisor` | `@Dependent`, the doer method of the Sim task |
| `BusDriver` | `@ApplicationScoped`, the doer methods of the Bus task |
| `SimRepository`, `BusRepository` | `@ApplicationScoped`, loader and saver, JDBC and JSON-B; `BusRepository` is `@Transactional` (see [Rules](#rules-of-the-code)) |
| `Simulation`, `Bus`, `SimStatus`, `BusStatus` | data and calculation, status names |

## Validation classes

| Aspect | Classes | Tested by |
|---|---|---|
| generated code | `GeneratedCodeMethods`, `GeneratedCodeFailures`, `GeneratedCodeTaskData`, `TaskRunner`, `CallTrace` | `GeneratedCodeTest` |
| kinds of beans | `BeanKindSingleton`, `BeanKindProduced` with `BeanKindProducers`, `BeanKindIntercepted` with the binding `@BeanKindTraced` and `BeanKindTracedInterceptor`, `BeanKindConstructor`, `BeanKinds.Nested`, `BeanKindInherited` extends `BeanKindBase` | `BeanKindsE2E` |
| doer methods | `DoerMethodStatuses` (hand-over to `DoerMethodNextClass`, delay, statuses from constants of `DoerMethodStatusNames`, `switch`, lambda), `DoerMethodCalls` (inserts a task with `DoerService`) | `DoerMethodsE2E` |
| concurrency | `ConcurrencyLimitOne`, `ConcurrencyQueues` (`@Dependent`, limits on the class and on a method, a slow and a failing method in one domain), `ConcurrencyGroupFirst`, `ConcurrencyGroupSecond` (one group in two classes) | `ConcurrencyE2E` |
| transactions | `TransactionMethods`: loader and saver of `TransactionData`, which write `demo_log_tasks` with `txid_current()`; `updateAndBumpVersion` in a doer method; a doer method with its own `@Transactional` | `TransactionsE2E`, `CoordinatedUpdateE2E` |
| task data | `TaskDataMethods`: the loader of the record `TaskDataRecord`, without a saver | `DoerMethodsE2E` |
| errors | `ErrorMethods` (checked and runtime exceptions, `@RetryPolicy` with a fallback and without a duration, cause and suppressed, the describer of `RuntimeException`), `ErrorDescribers` (the describer of `Exception`) | `ErrorsE2E` |
| external service | `ExternalServiceMethods` (JAX-RS client: success, error, timeout), `ExternalServiceConfig` (URL from `E2E_EXTERNAL_URL`) | `ExternalServiceE2E` |
| validation endpoints | `ValidationResource` (`@ApplicationScoped`; also doer methods of a JAX-RS resource) | all `*E2E` |

### Validation endpoints

`ValidationResource`, under `/api/validation`, lets the tests control Doer and read its state:

| Endpoint | Does |
|---|---|
| `GET info` | `{"runtime": "<E2E_RUNTIME>"}`: the application is up, migrated, and runs Doer |
| `GET start?m=`, `stop` | starts Doer (with the monitor of delayed tasks when `m=true`), stops it |
| `GET reset?m=` | stops Doer, deletes all tasks, logs and data, starts Doer again |
| `GET load`, `check`, `fix` | reloads the queues, checks the tasks that became ready, resets stalled tasks |
| `GET add_task?s=`, `task?id=` | inserts a task with a status, reads a task |
| `POST run-task` | runs one task through `TaskRunner` (below) |
| `GET coordinated_update…` | `facilitateCoordinatedUpdate` with and without task data, with a type without a loader, and inside a transaction (both must fail) |

### `TaskRunner`: one protocol for two suites

`GeneratedCodeTest` runs both in `*GeneratedCodeITCase` (no container, no database) and in `GeneratedCodeE2E` (a runtime). Both send the same request to the same `TaskRunner`, which inserts a task, calls `doerService.runTask(task)` and answers with the final status, whether `failingSince` is set, and the calls that `CallTrace` recorded for this task:

```
→ {"status": "Generated code 2 params"}
← {"status": "Generated code 2 params done", "failing": false, "trace": ["GeneratedCodeTaskData.loadFirst", "GeneratedCodeMethods.twoParams(First[loadFirst])", "GeneratedCodeTaskData.saveFirst(First[loadFirst, twoParams])"]}
→ {"status": "Generated code runtime exception", "failingFor": "PT25H"}
← {"status": null, "failing": false, "trace": ["GeneratedCodeFailures.runtimeException"]}
```

Task data records who got it, so the trace shows that the method and the saver get the object of the loader. `failingFor` is a duration, so the clocks of the test, the application and the database need not agree. In `*GeneratedCodeITCase` the requests are lines of JSON on the stdin of `transitsims.Main` (`TransitSimsProcess.MAIN`, with a `TestDoerService` instead of a database); in e2e they are the body of `POST /api/validation/run-task`, and `TaskRunner` inserts the task already in progress, so that the scheduler of the running Doer skips it.

## What it covers

The map of the cases: a new case gets a row here and its class in Transit Sims.

### Generated code (`GeneratedCodeTest`)

What `runTask` of the generated service does. The variety of annotations is the job of `GeneratorITCase`.

| Case | Code |
|---|---|
| the providers of the beans are injected into the generated service (`_inject_*`), the bean is looked up on first use, and the method of the right bean is called | `GeneratedCodeMethods`, `GeneratedCodeFailures`, `GeneratedCodeTaskData` |
| doer method with 1 parameter: `Task` | `GeneratedCodeMethods.oneParam` |
| 2 parameters, `Task` not first; a loader in a bean without doer methods | `GeneratedCodeMethods.twoParams` ← `GeneratedCodeTaskData.loadFirst` |
| 3 parameters; 2 loaders in different beans | `GeneratedCodeMethods.threeParams` ← `GeneratedCodeTaskData.loadFirst`, `GeneratedCodeMethods.loadSecond` |
| savers get the objects of the loaders; no saver after an exception | `GeneratedCodeTaskData.saveFirst`, `GeneratedCodeMethods.saveSecond` |
| a checked or runtime exception: `failingSince` set, the status back to the initial one | `GeneratedCodeFailures` |
| still failing 1 day after `failingSince`, no `@RetryPolicy`: status `null` | `GeneratedCodeFailures.runtimeException` |
| still failing after `@RetryPolicy(duration)`: `fallbackStatus` | `GeneratedCodeFailures.retryPolicy` |

### Beans with doer methods

| Case | Why it matters | Code |
|---|---|---|
| `@ApplicationScoped` | the generated service gets a client proxy | `BusDriver`, `ConcurrencyLimitOne`, `TransactionMethods`, `ErrorMethods` |
| `@Dependent` | no proxy; one instance shared by all Doer threads | `SimSupervisor`, `ConcurrencyQueues` |
| `@jakarta.inject.Singleton` | a pseudo-scope, and not a bean defining annotation in CDI | `BeanKindSingleton` |
| a producer | the class has no bean defining annotation | `BeanKindProduced` ← `BeanKindProducers.produce` |
| an interceptor binding on a doer method | the method is called through the interceptor | `BeanKindIntercepted`, `@BeanKindTraced`, `BeanKindTracedInterceptor` |
| a doer method with its own `@Transactional` | its transaction is apart from Doer's | `TransactionMethods.ownTransaction` |
| an `@Inject` constructor | CDI also needs a no-arg one for the proxy | `BeanKindConstructor` |
| a bean that injects `DoerService` through its constructor | a circular reference that Spring does not resolve; the generated service injects a `Provider` of the bean | `DoerMethodCalls` |
| a JAX-RS resource with doer methods | it needs a scope (see [Rules](#rules-of-the-code)) | `ValidationResource` |
| a bean in another package, a nested class, inherited doer methods | names and imports in the generated service | `transitsims.validation`, `BeanKinds.Nested`, `BeanKindInherited` |

### Doer methods

| Case | Code |
|---|---|
| several `@AcceptStatus` on one method | `ConcurrencyLimitOne.slow` |
| `@AcceptStatus` with `delay`; the status stays, the method runs again | `BusDriver.step`, `SimSupervisor.checkCompletion`, `DoerMethodStatuses.delayed` |
| status set to `null` | `SimSupervisor.checkCompletion`, `BusDriver.step`, `BusDriver.resume`, `ValidationResource.second` |
| statuses from constants of another class, from `switch` and lambdas | `BusStatus`, `SimStatus`, `DoerMethodStatuses` |
| `@RetryPolicy` without `duration` | `ErrorMethods.retryForever` |
| calls `DoerService` itself | `DoerMethodCalls.insertTask`, `TransactionMethods.updateInMethod` |
| calls an external service: success, error, timeout | `ExternalServiceMethods` |
| long-running | `ConcurrencyLimitOne`, `ConcurrencyQueues.slow` |

### Concurrency

| Case | Code |
|---|---|
| `@ConcurrencyLimit` on a class and on a method | `ConcurrencyQueues`, `ConcurrencyQueues.chainMethod` |
| `@ConcurrencyGroup` of the class itself next to `@ConcurrencyLimit`, and on a method of another class that joins the group | `ConcurrencyGroupFirst`, `ConcurrencyGroupSecond.slow` |
| moving between domains: class → method → class | `ConcurrencyQueues.chainClass` → `chainMethod` → `chainClassAgain` |

### Task data and coordinated updates

| Case | Code |
|---|---|
| loader and saver in the bean of the doer method | `TransactionMethods.load`, `save` |
| loader and saver in a repository bean without doer methods (JDBC) | `SimRepository`, `BusRepository`, `GeneratedCodeTaskData` |
| loader without a saver; a record as task data | `TaskDataMethods.load`, `TaskDataRecord` |
| loader and saver join Doer's transaction | `TransactionMethods` (`txid_current()`) |
| `facilitateCoordinatedUpdate` with and without task data; a type without a loader, and a call inside a transaction, fail | `ValidationResource.coordinatedDataUpdate`, `coordinatedUpdate`, `coordinatedUpdateWithoutLoader`, `coordinatedUpdateInTransaction` |
| a coordinated update that updates other tasks, with hijacking | `SimResource.start`, `pause`, `resume` |
| a coordinated update by many clients at once for one task | `BusResource.board`, `alight` |
| a `@Transactional` method that inserts tasks and data in one transaction | `SimRepository.create` |

### Exception describers

| Case | Code |
|---|---|
| in a bean of its own; in a bean with doer methods | `ErrorDescribers`, `ErrorMethods.describeRuntimeException` |
| for `RuntimeException` next to one for `Exception` | `ErrorMethods.describeRuntimeException`, `ErrorDescribers.describeException` |
| an exception with a cause and suppressed exceptions | `ErrorMethods.causeAndSuppressed` |

### Lifecycle and configuration

| Case | Code |
|---|---|
| Doer started on `Startup`, stopped on `Shutdown` | `DoerLifecycle` |
| restart with no tasks in progress | `SmokeE2E` |
| restart while a simulation runs | `LifecycleE2E` |
| two nodes on one database | `LifecycleE2E` |
| URLs of external services from environment variables | `ExternalServiceConfig` |
| stop while a doer method runs | not covered: the task stays `in_progress` (see [Rules](#rules-of-the-code)) |

## Rules of the code

What the code of Transit Sims follows, and why. A Doer user meets the same questions; they are material for the documentation of Doer.

- **Validation only in `transitsims.validation`.** The simulation knows nothing of the tests; `demo_log_tasks` is written only by `TransactionMethods`.
- **A JAX-RS resource with doer methods has a scope** (`ValidationResource` is `@ApplicationScoped`). Doer threads have no request context: without a scope a resource is per request in WildFly and Open Liberty and no Spring bean in Spring Boot, and the generated service could not call it. For the same reason a doer method is never in a `@RequestScoped` bean.
- **A JAX-RS resource without a scope** (`SimResource`, `BusResource`) is fine where RESTEasy makes it a CDI bean; with Jersey and CDI it needs a scope or the bean discovery mode `all` (as in Jetty).
- **`@jakarta.inject.Singleton` alone does not make a CDI bean** (nor does `@Named`): a WAR without `beans.xml` does not discover `BeanKindSingleton`. Quarkus does; the servers get a producer, Jetty the mode `all`.
- **Reads that precede a coordinated update run in a transaction** (`BusRepository` is `@Transactional`). In Open Liberty a connection taken outside a transaction stays with the request until it ends, so reading a bus and then updating it needed two connections at once, and concurrent boardings exhausted the pool.
- **No qualifiers, decorators or alternatives** on beans with doer methods: the generated service injects by type.
- **Beans with doer methods may inject `DoerService`**, also through the constructor (`DoerMethodCalls`). The generated service injects `Provider`s of the beans and of itself and looks the beans up on first use, so Spring Boot needs no `spring.main.allow-circular-references`.
- **Doer runs with the monitor** (`DoerLifecycle` starts it with `start(true)`), so a delayed task runs after its delay. `E2eEnvironment` stops Doer right after the first start of a node, so that the tests of aspects control Doer through the validation endpoints; a node restarted by a test keeps Doer running.
- **Open: a doer method in progress when the node stops** stays `in_progress` until the reset of stalled tasks (2 hours): `DoerService.stop()` does not wait for it, and the runtime may stop its transactions before the method ends.
