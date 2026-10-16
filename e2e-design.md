# End-to-end functional testing

Status: steps 1–6 implemented. The details of how they were worked through are in the git history of this file.

Doer is tested by two questions that need different test matrices:

| Question | Suite | Matrix |
|---|---|---|
| Does the annotation processor generate correct code with every JDK and build tool? | `*GeneratedCodeITCase` | JDK 17, 21, 25, 27 × javac, Maven |
| Does the generated code work in a real application in every runtime? | e2e suite (`*E2E`) | Quarkus, Spring Boot, WildFly, Open Liberty, Jetty; JDK 25 |

Both ask it about the same code: [Transit Sims](src/test/resources/e2e/transitsims/README.md), a Jakarta EE application that uses Doer. How the tests are built and run is in [src/test/README.md](src/test/README.md).

## Done

1. **Infrastructure on Quarkus.** `E2eEnvironment`: Postgres and WireMock in Docker, the application as `docker run` of an artifact built by Maven in a work folder, restarts and a second node; the Maven profile `e2e`; the CI job `runtime-e2e`. `QuarkusITCase` and its fragile project generation are gone.
2. **The generated code on the same sources.** `GeneratedCodeTest` runs the same checks with javac, Maven and in every runtime.
3. **Transit Sims** replaced CarWash: a simulation of buses and passengers, and validation classes for every aspect of Doer; the map of the cases is in its README.
4. **Spring Boot**, with Jersey; what Spring does differently from CDI is in a generated `SpringBootMain.java`.
5. **WildFly and Open Liberty**, as WARs in the images of the servers.
6. **Jetty**, a servlet container: the WAR brings Weld, Jersey, Narayana and Agroal.

Doer was fixed on the way: the idle monitor wakes up for a task queued with a delay, and the caller of a coordinated update skips the frames of the transaction interceptors of WildFly and Open Liberty.

## Open questions

- **A doer method in progress when the node stops** stays `in_progress` until the reset of stalled tasks (2 hours): `DoerService.stop()` does not wait for it, and the runtime may stop its transactions first. Options: `stop()` interrupts and waits with a short timeout; the application gives Doer an executor it stops itself; a documented limitation.
- **Timing-dependent tests** (`ConcurrencyE2E.queues__should_grow_and_shrink`, `concurrency_10_should_run_10_methods_in_parallel`) sometimes fail with 4 CPUs in WildFly and Open Liberty, and once a doer method of `DoerMethodsE2E` hung in Quarkus in CI (the test now takes a thread dump). They stay until Doer's scheduling is improved.
- **Doer components in a dependency jar** (Quarkus needs a Jandex index, a WAR `beans.xml` in the jar): a second layout of Transit Sims, or out of scope?
- **Which runtimes are supported**: candidates Payara, TomEE, Helidon MP; Micronaut is not Jakarta EE, but would test whether its own processor sees the generated service.

## Plan

1. **Review of the tests.** Each `*E2E` class against the map of the cases of Transit Sims: what it checks, overlaps, gaps, assertions that depend on timing.
2. **Renaming.** Names of test classes and methods, statuses and validation endpoints left from earlier test applications (`Parked`/`Unparked`, `demo_log_tasks`, `add_task` next to `run-task`), made consistent.
3. **Polishing the e2e runtimes.** The settings each runtime needed (marked with comments in the `E2eApp` classes, listed in [src/test/README.md](src/test/README.md#runtimes)) become the documentation for Doer users, or are removed by fixes in Doer where Doer is the cause: the self-reference that Spring Boot forbids, frames found by package names, a warning of the processor for a bean CDI does not discover, the size of the executor Doer needs.
4. **More build tools** in `*GeneratedCodeITCase`: Gradle (a pinned distribution or the one on the runner?), the Eclipse compiler (ecj).
5. **More runtimes**, as decided in the open question; each is an `E2eApp` and a job of the CI matrix.
