# End-to-end functional testing

Status: proposed

Doer is tested by two questions that need different test matrices:

1. **Does the annotation processor generate correct code with every supported JDK and build tool?** The answer does not depend on the runtime the application is deployed to. It needs the JDK matrix (17, 21, 25, 27) and several build tools, but no database and no container.
2. **Does the generated code work in a real application in every supported runtime?** The answer barely depends on the JDK. It needs a database, external services and a running application in Quarkus, Spring Boot, WildFly and others, but only one JDK.

Both questions are asked about the same code: **Transit Sims**, a Jakarta EE backend application that uses Doer.

| Suite | What it checks | Matrix | How it uses Transit Sims |
|---|---|---|---|
| `*GeneratedCodeITCase` | the annotation processor | JDKs × build tools (javac, Maven, Gradle) | adds its own `Main`, runs `GeneratedCodeTest` without a database |
| e2e suite (`*E2E` classes) | the generated code in a deployed application (`GeneratedCodeE2E`: the same `GeneratedCodeTest`), and how the runtime serves it | runtimes (`quarkus`, `spring-boot`, `wildfly`, …), one JDK | adds DB migrations and a runtime setup, runs it in Docker, tests it over REST, JDBC and WireMock |

This document describes only the infrastructure: the Transit Sims project, how each suite builds and runs it, and CI. What Transit Sims does and what exactly the suites check is designed in [e2e-test-app.md](e2e-test-app.md). Transit Sims replaces CarWash, the first test application, in [step 3](#3-transit-sims).

`GeneratorITCase`, `GeneratorErrorsITCase`, `DoerServiceJdbcITCase` and the unit tests stay in the JDK matrix. The kinds of tests and how they are built are in [src/test/README.md](src/test/README.md).

## Problems today

- **Only Quarkus is tested.** Nothing tells whether the generated service works where CDI, JTA or the `DataSource` behave differently: WildFly, Open Liberty, Spring Boot.
- **`QuarkusITCase` runs in every JDK of the matrix**, so the slowest test is built and run 4 times, although it tests the runtime, not the JDK.
- **The application is assembled in a fragile way.** `QuarkusITCase` generates a project with `quarkus:create` on every run, patches its `pom.xml` with regular expressions, and builds it twice to get the Flyway script.
- **The processor and the runtime are tested on different code.** `CarWashITCase` has its own toy classes in text blocks, and `QuarkusITCase` uses `src/test/resources/e2e-code`. Code that compiles and runs in `CarWashITCase` is not the code that runs in Quarkus.

## Overview

```
                src/test/resources/e2e/transitsims   (Transit Sims: com.doer + jakarta.* APIs only)
                                 │
         ┌───────────────────────┴──────────────────────────┐
         │                                                  │
 *GeneratedCodeITCase                       E2eEnvironment, -Ddoer.e2e.runtime=<runtime>
 + Main (text block)                        copySources → configure<Runtime>App → mvn package
 + TestDoerService (no DB)                  → check migrations → docker run --rm (Flyway)
 javac / Maven / Gradle                                     │
 × JDK 17, 21, 25, 27                   ┌──────────── Docker network ─────────────┐
                                        │  Transit Sims (1+)  postgres  wiremock  │
                                        └─────▲──────────────────▲─────────▲──────┘
                                              │ REST             │ JDBC    │ admin API
                                          e2e test classes (*E2E), one JDK
```

## Transit Sims

Transit Sims is a Jakarta EE backend application that simulates buses and passengers on a city transport network, with Doer running the buses (see [e2e-test-app.md](e2e-test-app.md)). It has a short functional part, and specialized classes in `transitsims.validation` for every aspect of Doer the functional part does not cover. For the infrastructure, what matters is that it is **one application with a set of components of every kind a Doer user writes**, including REST endpoints.

### Components

| Kind | Purpose |
|---|---|
| CDI beans (`@ApplicationScoped`, `@Dependent`, …) with doer methods | business operations: `@AcceptStatus`, `@RetryPolicy`, `@ConcurrencyLimit`, `@ConcurrencyGroup` |
| `@TaskDataLoader`, `@TaskDataSaver`, `@ExceptionDescriber` methods | in the same beans as the doer methods and in other beans |
| **functional REST endpoints** (JAX-RS) | the API of Transit Sims for its clients; start business operations, coordinated updates (`facilitateCoordinatedUpdate`) |
| **validation REST endpoints** (JAX-RS) | for the tests only: control Doer (start, stop, reload, check ready tasks, reset stalled tasks), read tasks, reset data, report the runtime the application runs in. `ValidationResource` |
| repositories | JDBC through the injected `DataSource` |
| clients of external services | JAX-RS client API (`jakarta.ws.rs.client`); in e2e the services are simulated by WireMock |
| `DoerLifecycle` | `@Observes Startup` → `doerService.start(true)` (with the monitor of delayed tasks), `@Observes Shutdown` → `stop()` |
| `ExternalServiceConfig` | URLs of external services from environment variables (`System.getenv`); MicroProfile Config is missing in Spring Boot and Jetty |

All endpoints, functional and validation, are in one application under `@ApplicationPath("/api")`; the validation endpoints have their own path, `/api/validation`.

### The one rule

**Transit Sims imports only `com.doer`, Jakarta Web Profile APIs (CDI, JTA, JAX-RS, JSON-P, JSON-B) and `javax.sql`.** Nothing of Quarkus, Spring or WildFly. Then the same sources compile in `*GeneratedCodeITCase` and run in every runtime.

Today's `DoerResource` breaks the rule with `io.quarkus.runtime.StartupEvent`; it is replaced by the CDI 4 `jakarta.enterprise.event.Startup` event (Jakarta EE 10), which Quarkus, WildFly and Open Liberty fire.

`Main` of `*GeneratedCodeITCase` calls only `TaskRunner` and the GeneratedCode* beans (see [e2e-test-app.md](e2e-test-app.md#the-same-tests-in-generatedcodeitcase-and-in-e2e)). Every other component is only created by `Main` and injected into the generated service, never called. So every component must be constructible without a container. `Main` is in the package `transitsims`, so components of other packages (`transitsims.validation`) get their collaborators through `@Inject` setters or a public constructor.

### Layout

Transit Sims stays in test resources. `e2e/transitsims` is the folder of the Java package `transitsims`: it is copied to `<workdir>/src/main/java/transitsims` as it is. DB migrations are separate: only the e2e suite uses them; they are copied to `<workdir>/src/main/resources/db/migration`, the default location of Flyway.

```
src/test/resources/e2e/
  transitsims/                      package transitsims: functional code, @ApplicationPath("/api") → <workdir>/src/main/java/transitsims
    validation/                     package transitsims.validation: specialized classes, validation REST endpoints (/api/validation)
  db/migration/
    V1__create_doer_schema.sql        = generated CreateSchema.sql: Doer tables and sequences
    V2__create_doer_indexes.sql       = generated CreateIndexes.sql: Doer indexes for Transit Sims statuses
    V3__create_validation_tables.sql  tables and triggers needed by the tests
```

Transit Sims has no tests of its own: the suites test Transit Sims, not tests inside it.

`V1` and `V2` are what a Doer user would commit: copies of `CreateSchema.sql` and `CreateIndexes.sql`, one file each, as the processor generates them for Transit Sims. One file per generated file keeps them comparable as they are. The e2e suite compares each of them with its generated file in the work folder, fails on a difference and names the generated file to copy over it; so a change of Transit Sims that changes the schema (a new status, a new concurrency group) also changes the migration in the same commit. The application sets up the database itself: it runs these migrations with Flyway at start, before Doer starts (see [What each runtime writes](#what-each-runtime-writes)).

`src/test/resources/e2e-code` is moved here: its Java code becomes the first version of CarWash, its trigger SQL `V3__create_validation_tables.sql`.

**No shared glue folders**, at least at first. What a runtime cannot work without — `pom.xml`, configuration, a `SpringBootApp` class, a `DataSource` producer — is written by that runtime's methods in the test code, as text blocks (see [Runtime setup](#runtime-setup)). When two runtimes need the same text (the producers of WildFly and Open Liberty), it is a shared constant in the test code, not a folder.

## `*GeneratedCodeITCase`: the annotation processor

Each build tool is a class that implements `GeneratedCodeTest` (see [e2e-test-app.md](e2e-test-app.md#the-same-tests-in-generatedcodeitcase-and-in-e2e)). In `@BeforeAll` it copies `src/test/resources/e2e/transitsims` into its workspace and builds it together with two text blocks:

- `TEST_DOER_SERVICE` — the generated service without a database;
- `TransitSimsProcess.MAIN` — wires the Transit Sims components by hand (the `_inject_*` methods of the generated service) and passes each request to `TaskRunner`.

Transit Sims is compiled against the Jakarta EE API jar, which `Toolchain` resolves. The REST endpoints and JDBC repositories are compiled but not run by `Main`. No database is needed.

| Build tool | Class |
|---|---|
| javac | `JavacGeneratedCodeITCase` |
| Maven | `MavenGeneratedCodeITCase`: `pom.xml` as a text block, sources copied in, processor in `annotationProcessorPaths` |
| Gradle | new `GradleGeneratedCodeITCase`: `build.gradle` as a text block with `annotationProcessor "com.java-doer:doer:…"` and `mavenLocal()`; a pinned Gradle version |
| Eclipse compiler (ecj) | candidate: the compiler of Eclipse and VS Code Java; runs processors differently from javac |

That the processor generates nothing when it compiles test sources (`DoerProcessor.isCompilingTests`) is checked by `GeneratorITCase` with two javac runs, as a build tool compiles main and test sources. The build tools are not checked for it: if javac is right, they are right too.

The `*GeneratedCodeITCase` classes stay in the JDK matrix, so they run with JDK 17, 21, 25 and 27.

## The e2e suite: the generated code in runtimes

### Selecting the runtime

```
mvn verify -Ddoer.e2e.runtime=quarkus
```

| Property | Meaning |
|---|---|
| `doer.e2e.runtime` | `quarkus`, `spring-boot`, `wildfly`, `open-liberty`, `jetty`, or `external`. Not set: the e2e suite is not run. |
| `doer.e2e.skip-build` | `true`: reuse `target/e2e/<runtime>` from the previous run (fast local iteration) |
| `doer.e2e.base-url`, `doer.e2e.jdbc-url`, `doer.e2e.wiremock-url` | with `runtime=external`: test an application that is already running (the work folder started from the IDE, under a debugger) |

A Maven profile `e2e`, activated by the property `doer.e2e.runtime`, adds a failsafe execution with `<includes>**/*E2E.java</includes>` and `reuseForks=true` (one environment for all e2e classes), and skips surefire and the default failsafe execution: the e2e job does not repeat what the JDK matrix already ran. Without the profile, `*E2E.java` does not match the failsafe defaults, so the e2e classes are never run by accident.

When an e2e class is run from the IDE without the property, the harness uses `quarkus`.

### Runtime setup

Each runtime is one class in `src/test/java/com/doer/e2e/` that turns the Transit Sims sources into a Maven project and says how to run its artifact in Docker:

```java
class QuarkusApp implements E2eApp {
    @Override
    public void configure(Path workdir) throws Exception {
        copySources("e2e/transitsims", workdir.resolve("src/main/java/transitsims"));
        copySources("e2e/db/migration", workdir.resolve("src/main/resources/db/migration"));
        configureQuarkusApp(workdir);
    }

    @Override
    public DockerRun dockerRun(Path workdir) {
        return quarkusDockerRun(workdir);
    }

    void configureQuarkusApp(Path workdir) throws IOException {
        writeFile(workdir, "pom.xml", """
                <project>
                  ...
                  <dependency>
                    <groupId>com.java-doer</groupId>
                    <artifactId>doer</artifactId>
                    <version>%s</version>
                  </dependency>
                  ...
                </project>
                """.formatted(doerLibVersion));
        writeFile(workdir, "src/main/resources/application.properties", """
                quarkus.datasource.db-kind=postgresql
                quarkus.datasource.jdbc.url=${E2E_DB_URL}
                quarkus.flyway.migrate-at-start=true
                ...
                """);
    }

    /** Options (mounts of the artifact), image and command of `docker run`; the application listens on 8080. */
    DockerRun quarkusDockerRun(Path workdir) {
        return new DockerRun(List.of("-v", workdir.resolve("target/quarkus-app") + ":/app:ro"),
                "eclipse-temurin:25-jre", List.of("java", "-jar", "/app/quarkus-run.jar"));
    }
}
```

- **The whole `pom.xml` is a text block**, not an archetype patched with regular expressions. The versions of the runtime and of Doer are its only parameters.
- **No Dockerfiles.** The container is the runtime's official image (or a JRE image) with the built artifact mounted read-only (`-v …:ro`): `quarkus-app/` to a JRE image, `ROOT.war` to WildFly's `standalone/deployments`, `ROOT.war` and `server.xml` to Open Liberty's `config/`.
- **The application is a process, not a Testcontainers container**: `E2eEnvironment` runs `docker run --rm` with `ProcessBuilder` (see [Environment](#environment)). The runtime gives only what differs: mounts, image and command. Everything else — name, network, port, `E2E_*` variables — is added by `E2eEnvironment`, the same for all runtimes. Every runtime listens on 8080 in the container.
- **The work folder `target/e2e/<runtime>/` is a complete Maven project** after `configure`. To debug, open it in the IDE, run it, and point the tests at it with `doer.e2e.runtime=external`. Fixes are copied back to `src/test/resources/e2e/transitsims` by hand.
- `mvn package` in the work folder is the same for all runtimes and uses the local repository of the outer build (`-Dmaven.repo.local` from `doer.test.m2.repo`).
- **Doer comes in as `0.0.0-IT-SNAPSHOT`** (`doer.lib.version`), the jar of the current build: the execution `install-doer-for-it` of the outer `pom.xml` installs it into the local repository in `pre-integration-test`, before the e2e suite runs. An e2e class run from the IDE uses what the last `mvn verify` (or `mvn -DskipTests verify`) installed.

### What each runtime writes

The generated `_GeneratedDoerService` needs a `DataSource` bean, an `Executor` bean, `@jakarta.transaction.Transactional` interceptors and CDI-style injection. Transit Sims needs JAX-RS and the `Startup` / `Shutdown` events. The database must be migrated by Flyway before Doer starts. Where the runtime gives them, nothing is written; otherwise the runtime's `configure…App` writes the smallest file that fills the gap:

| Runtime | Provided by the runtime | Written by the test |
|---|---|---|
| **quarkus** | `DataSource` (Agroal), `Executor`, JTA (Narayana), CDI events, RESTEasy, Flyway (`quarkus-flyway`, `migrate-at-start`) | `pom.xml`, `application.properties` |
| **wildfly** | JTA, CDI events, RESTEasy | `pom.xml` (WAR), `WEB-INF/transitsims-ds.xml` with `${env.E2E_DB_URL}`, `Producers.java`: `DataSource` from `@Resource(lookup)`, `Executor` from `ManagedExecutorService`; `FlywayMigration.java` |
| **open-liberty** | JTA, CDI events, JAX-RS | `pom.xml` (WAR), `server.xml` (features, data source, Postgres driver), the same `Producers.java` and `FlywayMigration.java` |
| **spring-boot** | `DataSource` (Hikari), transactions (Spring reads the jakarta annotation), Flyway (auto-configured with `flyway-core`) | `pom.xml`, `application.properties`, `SpringBootApp.java` (see below) |
| **jetty** | servlet container only | `pom.xml`, Weld servlet, Jersey and Narayana setup, `web.xml`, producers of a transactional `DataSource` and an `Executor`, `FlywayMigration.java` |

`FlywayMigration.java`, for the runtimes without a Flyway integration, observes `Startup` with a `@Priority` lower than the one of `DoerLifecycle`, so that CDI calls it first, and runs `Flyway.configure().dataSource(ds).load().migrate()`. Flyway is a dependency of the runtime's `pom.xml`, not of Transit Sims: Transit Sims does not import `org.flywaydb`. In Quarkus and Spring Boot the migration runs before the `Startup` event / `ApplicationReadyEvent`, so nothing is written. Every runtime also needs `flyway-database-postgresql`.

`SpringBootApp.java` carries everything Spring does differently from CDI, so that Transit Sims stays free of Spring:

- `@Import(_GeneratedDoerService.class)`, because Spring does not know `@ApplicationScoped`;
- a component scan of `transitsims` with an include filter on CDI scope annotations, and a `ScopeMetadataResolver` that maps `@ApplicationScoped` → singleton, `@Dependent` → prototype;
- the `DataSource` wrapped in `TransactionAwareDataSourceProxy`: on a plain `DataSource`, `getConnection()` does not join the Spring transaction;
- exactly one `Executor` bean: with two, `@Inject Executor` is ambiguous;
- a Jersey `ResourceConfig` that registers the JAX-RS endpoints;
- `ApplicationReadyEvent` → `doerService.start(false)`, `ContextClosedEvent` → `stop()`: Spring does not fire the CDI events, so the observer in `DoerLifecycle` is never called.

### Environment

`E2eEnvironment` is a JUnit extension with one instance per JVM. On first use it:

1. starts a Testcontainers `Network`, Postgres (alias `db`) and WireMock (alias `wiremock`), and points the static WireMock client to the mapped admin port (`WireMock.configureFor`);
2. picks the `E2eApp` for `doer.e2e.runtime` and calls `configure(target/e2e/<runtime>)`;
3. runs `mvn -B package` in the work folder;
4. checks that `V1__create_doer_schema.sql` equals `CreateSchema.sql` and `V2__create_doer_indexes.sql` equals `CreateIndexes.sql`, generated in `target/classes/com/doer/generated` of the work folder; on a difference it fails and names the generated file to copy. It does not touch the database: the application migrates it with Flyway at start. Because the generated SQL is committed, the double build of `QuarkusITCase` goes away;
5. starts node 1 as a process with `ProcessBuilder`:
   ```
   docker run --rm --name transitsims-<runtime>-<node>-<start> --network <network> -p 127.0.0.1:<port>:8080
       --label org.testcontainers.sessionId=<session>
       -e E2E_RUNTIME=<runtime> -e E2E_DB_URL=jdbc:postgresql://db:5432/doer -e E2E_DB_USER=… -e E2E_DB_PASSWORD=…
       <options> <image> <command> of E2eApp.dockerRun
   ```
   with stdout and stderr appended to `target/e2e/<runtime>/app-<node>-out.txt` and `app-<node>-err.txt`, and the command written to `app-<node>-cmd.txt`. The host port is picked once per node and kept across restarts. The URLs of the external services on WireMock are added as `-e` when Transit Sims gets them. The image is pulled with `docker pull` before, so that the wait below does not include the download;
6. waits until the validation endpoint `GET /api/validation/info` answers with the expected runtime (so the database is migrated and Doer is started); on a timeout, or when the process exits before, it fails with the tail of the log.

**Restarts.** `stopNode(node)` calls `process.destroy()`: SIGTERM to `docker run`, which passes it to the application in the container (`--sig-proxy` is on by default). Then `waitFor`, and both log files get a separator line with the timestamp, how long the stop took and the exit code. `--rm` removes the container. `startNode(node)` appends a separator line with the timestamp and runs `docker run` again into the same files. The container name carries the start number, because Docker removes the old container a moment after `docker run` exits. More nodes on the same database are started the same way, each with its own port and log files.

**Cleanup.** The application containers carry the Testcontainers session label, so Ryuk removes them, with Postgres, WireMock and the network, when the test JVM ends, even if it crashes; a shutdown hook also stops the running processes. The `docker` CLI must be on `PATH`.

Tests get three handles from it:

| Handle | Gives access to |
|---|---|
| **REST** (RestAssured, base URI of node 1) | functional and validation endpoints |
| **JDBC** (`DataSource` to the mapped Postgres port) | `tasks`, `task_logs` and Transit Sims tables |
| **WireMock** (static `WireMock.stubFor`, `verify`, …, configured for the mapped admin port) | stubs and received requests of the external services |

Plus `appLog(node)` for the application log (`app-<node>-out.txt`), and `stopNode` / `startNode`.

## CI

`maven.yaml` gets a second job. The existing `build` job is unchanged and no longer runs Quarkus (the e2e profile is not active). The matrix below is the target; it starts with `[ quarkus ]` and grows with the [Implementation plan](#implementation-plan).

```yaml
  e2e:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        runtime: [ quarkus, spring-boot, wildfly, open-liberty, jetty ]
    steps:
      - uses: actions/checkout@v7
      - name: Set up JDK 27 (for building)
        uses: actions/setup-java@v6
        with:
          java-version: '27'
          distribution: 'temurin'
      - name: Build with Maven
        run: mvn -B --color=always -DskipTests package
      - name: Set up JDK 25 (for e2e)
        uses: actions/setup-java@v6
        with:
          java-version: '25'
          distribution: 'temurin'
      - name: E2E ${{ matrix.runtime }}
        run: mvn -B --color=always verify -Ddoer.e2e.runtime=${{ matrix.runtime }}
      - name: Upload application logs
        if: failure()
        uses: actions/upload-artifact@v7
        with:
          name: e2e-${{ matrix.runtime }}
          path: target/e2e/
```

- **One JDK** (25, the latest LTS) for building Transit Sims and in the container image. A runtime that lags behind can stay on 21 with an `include` entry in the matrix and a JDK parameter of its `E2eApp`.
- **Runtime versions** are constants in the runtime's `E2eApp` class. Testing two versions of one runtime (e.g. a Jakarta EE 10 and an EE 11 WildFly) is a matrix `include` with `-Ddoer.e2e.runtime.version=…`, added only when needed.
- The Maven repository cache gets the runtime in its key: Quarkus, Spring Boot and WildFly bring different dependencies.
- The uploaded `target/e2e/<runtime>/` has both the application logs and the generated project, so a failure can be reproduced from the artifact.

## Implementation plan

The plan is worked through top to bottom. A box is ticked when its **Check** passes. When the work shows that the plan or the design is wrong, fix the design first, then the plan, and go on. Steps 1 and 2 are detailed; each later step is detailed when it is next.

### 1. Infrastructure on Quarkus

**1.1 CarWash sources** (`src/test/resources/e2e`)

- [x] Move `e2e-code/tst/demo/*.java` to `e2e/carwash/`, package `tst.demo` → `carwash`.
- [x] Move `DoerResource` to `e2e/carwash/validation/` (package `carwash.validation`), path `/doer` → `/validation`; add `CarWashApplication` with `@ApplicationPath("/api")`.
- [x] Add `GET /api/validation/info`: `{"runtime": "<E2E_RUNTIME>"}`.
- [x] Move start / stop of Doer from `DoerResource` to `DoerLifecycle`, with `jakarta.enterprise.event.Startup` instead of `io.quarkus.runtime.StartupEvent`.
- [x] Replace `org.slf4j.Logger` in `CarWash` with `java.lang.System.Logger` (slf4j is not a Jakarta API).
- [x] Delete `e2e-code/test/tst/demo/DemoTest.java`: CarWash has no tests of its own.
- [x] `e2e/db/migration/V1__create_doer_schema.sql` and `V2__create_doer_indexes.sql`: copies of the generated `CreateSchema.sql` and `CreateIndexes.sql` of CarWash (replace the empty `V20240224_00__create_doer_tables.sql`).
- [x] `e2e/db/migration/V3__create_validation_tables.sql`: moved from `V20240224_01__create_e2e_test_triggers.sql`.
- [x] **Check:** `grep -rE "import (io\.quarkus|org\.springframework|org\.slf4j|org\.jboss)" src/test/resources/e2e` finds nothing.

**1.2 Maven**

- [x] Profile `e2e` in `pom.xml`, activated by the property `doer.e2e.runtime`: a failsafe execution with `<includes>**/*E2E.java</includes>`, `reuseForks=true` and the system properties `doer.e2e.runtime`, `doer.lib.version`, `doer.test.m2.repo`; surefire and the default failsafe execution skipped.
- [x] Test dependency of the WireMock client (`org.wiremock:wiremock-standalone`).
- [x] **Check:** `mvn verify` runs no `*E2E` class; `mvn verify -Ddoer.e2e.runtime=quarkus` runs only `*E2E` classes (with a placeholder test).

**1.3 `E2eEnvironment`**

- [x] `copySources(String resourceFolder, Path target)`: copies a folder of test resources; `writeFile` reuses `GeneratorTestBase.writeSource`.
- [x] `E2eApp` interface: `configure(Path workdir)`, `dockerRun(Path workdir)` → `DockerRun(options, image, command)`; `mvn -B package` in the work folder is common code (as `GeneratorTestBase.mvn`, with `-Dmaven.repo.local` of the outer build).
- [x] `E2eEnvironment`, a JUnit extension with one instance per JVM: `Network`; its own Postgres container with alias `db` (`Utils.getPostgresDataSource()` has no network); WireMock with alias `wiremock` and `WireMock.configureFor` (no test uses it yet); work folder `target/e2e/<runtime>`.
- [x] Checks that `V1__create_doer_schema.sql` equals the generated `CreateSchema.sql` and `V2__create_doer_indexes.sql` equals `CreateIndexes.sql` (on a difference: fails and names the generated file). The database is left to the application.
- [x] Node 1 as a `docker run --rm` process (`docker pull` before; name, network, port, session label, `E2E_*` variables); stdout and stderr appended to `app-1-out.txt` and `app-1-err.txt`. Waits for `GET /api/validation/info` with the expected runtime.
- [x] `stopNode(node)`: `process.destroy()`, `waitFor`, separator with timestamp, stop time and exit code; `startNode(node)`: separator with timestamp, `docker run` again into the same files; shutdown hook.
- [x] Handles: RestAssured base URI, `DataSource` of the e2e Postgres, static WireMock client, `appLog(node)`.
- [x] Properties: `doer.e2e.skip-build`; runtime `external` with `doer.e2e.base-url`, `doer.e2e.jdbc-url`, `doer.e2e.wiremock-url`; `quarkus` when the property is not set.

**1.4 `QuarkusApp`**

- [x] `configureQuarkusApp`: `pom.xml` as a text block (Quarkus BOM, `quarkus-resteasy`, `quarkus-resteasy-jsonb`, `quarkus-jdbc-postgresql`, `quarkus-flyway`, `flyway-database-postgresql`, `doer` as a dependency and in `annotationProcessorPaths`, `quarkus-maven-plugin`); `e2e/db/migration` copied to `src/main/resources/db/migration`; `application.properties` with `${E2E_DB_URL}`, `${E2E_DB_USER}`, `${E2E_DB_PASSWORD}` (defaults `localhost:5432/doer`, `doer`, `doer` for a run from the IDE), `quarkus.flyway.migrate-at-start=true`, port 8080, the log format of `QuarkusITCase`.
- [x] `quarkusDockerRun`: `target/quarkus-app` mounted to `/app`, `eclipse-temurin:25-jre`, `java -jar /app/quarkus-run.jar`.
- [x] **Check:** `SmokeE2E` (`GET /api/validation/info` answers `quarkus`; `flyway_schema_history` has V1, V2, V3) passes with `mvn verify -Ddoer.e2e.runtime=quarkus`, and again with `-Ddoer.e2e.skip-build=true`.
- [x] **Check:** `SmokeE2E` restarts node 1 with `stopNode` / `startNode`: the exit code is 143 (SIGTERM), `app-1-out.txt` has the separators and the logs of both starts, `/api/validation/info` answers again.

**1.5 Tests of `QuarkusITCase`**

- [x] Move the tests to a few `*E2E` classes by topic, without changing what they check; the REST and JDBC helpers (`pushTask`, `waitTaskStatus`, `restGetTask`, `loadDemoLogs`, …) to a shared class; JDBC through `E2eEnvironment` instead of `Utils`.
- [x] Paths `doer/...` → `/api/validation/...`; `quarkus__should_start` → `SmokeE2E` (no SmallRye Health).
- [x] `queues__should_grow_and_shrink` reads `appLog(1)` instead of `quarkus-app-out.txt`.
- [x] Timing bounds widened only where Docker needs it, each change noted in the commit message.
- [x] **Check:** all `*E2E` classes pass with `-Ddoer.e2e.runtime=quarkus` three runs in a row.
- [x] Delete `QuarkusITCase` and `src/test/resources/e2e-code`.

**1.6 CI**

- [x] Job `e2e` in `maven.yaml` with `runtime: [ quarkus ]`, as in [CI](#ci).
- [x] **Check:** the `build` job no longer builds Quarkus; the `e2e` job is green; on a failure, `target/e2e/` is uploaded.

### 2. `CarWashITCase` on the CarWash sources

javac and Maven only; more build tools are [step 7](#7-more-build-tools-in-generatedcodeitcase).

**2.1 CarWash and helpers**

- [x] `copySources` moves to `GeneratorTestBase` as a static method next to `writeSource`; `E2eApp.copySources` calls it.
- [x] `DoerResource`: `@Inject` setters instead of the package-private fields `doerService` and `ds`.
- [x] **Check:** all `*E2E` classes pass with `-Ddoer.e2e.runtime=quarkus`.

**2.2 `Main` and javac**

- [x] `Main` (package `carwash`, text block) wires every component by hand (`_inject_*` of a `TestDoerService` subclass that prints `writeTaskLog`), gives `CarWash` and `DoerResource` the recording `DataSource`, and runs through `runTask`, printing the status before and after:
  - `Car is dusty`: loaders of `Car` (in `DoerResource`, another package) and `Shampoo` (in `CarWash`, with the doer method), both savers;
  - `Car need polishing`: the second `@AcceptStatus` of `polishTheCar`, `Car` as the first parameter;
  - `Want a coffee` (`@Dependent` `Cafeteria`), `Need order pizza` (`PhoneBooth`), `A` → `B` → `null` (`DoerResource`);
  - `Should send email`: the exception, described by `ExceptionMapper` and `PhoneBooth` (the extra JSON in `writeTaskLog`);
  - `Should check email` with `failingSince` an hour ago: `@RetryPolicy` sets its fallback status.

  The expected output is a text block next to it.
- [x] javac: `e2e/carwash` copied to `<workspace>/carwash`, compiled with `TestDoerService.java` and `Main.java`, `carwash.Main` run. `writeCarWashClasses` and `writeCarWashMain` are removed: the processor details they covered (data types with the same simple name, loaders and savers in other classes) are tested in `GeneratorITCase`.
- [x] **Check:** `javac__should_build_working_car_wash` passes; `git grep -n "demo.test2\|writeCarWash" src/test/java` finds nothing.

**2.3 Maven**

- [x] `pom.xml` as a text block instead of the archetype: release 17, `doer` and `jakarta.jakartaee-api`, `parsson` at runtime (JSON-P for the extra JSON), `doer` in `annotationProcessorPaths`; CarWash, `TestDoerService` and `Main` in `src/main/java`; `mvn package`, then `exec:java` of `carwash.Main` with the same expected output.
- [x] A test source folder with one class with a doer method (no JUnit: only compiling it matters).
- [x] **Check:** `maven__should_build_working_car_wash` passes; the `mvn package` log has the note of `DoerProcessor.isCompilingTests`; `target/test-classes` and `target/generated-test-sources` have no `com/doer/generated`.
- [x] **Check:** `mvn verify` passes with JDK 17 and the newest JDK of the matrix.

**2.4 Test layout** (see [src/test/README.md](src/test/README.md))

- [x] `CarWashITCase` becomes `GeneratedCodeTest` with `JavacGeneratedCodeITCase`, `MavenGeneratedCodeITCase` and `GeneratedCodeE2E`, in the package `com.doer.generatedcode`.
- [x] `GeneratorTestBase` and `Utils` are replaced by `com.doer.testkit`: static helpers, and `InWorkspace` with `@TempDir(factory = Workspaces.class)` instead of a base class.
- [x] The check of test sources moves from the Maven build to `GeneratorITCase`, with two javac runs.
- [x] The Generator* beans of `carwash.validation` are renamed GeneratedCode*, after the test that uses them.

### 3. Transit Sims

Transit Sims replaces CarWash. It has two parts (see [e2e-test-app.md](e2e-test-app.md)):

- **the functional code**, package `transitsims`: what [transit-sims.md](transit-sims.md) describes, kept clear and short. Nothing in it exists for the tests;
- **specialized classes**, package `transitsims.validation`: one set of classes per aspect of Doer, named after it, as the GeneratedCode* classes are today. They cover every case the functional code does not.

Sub-steps 3.1 and 3.2 are detailed. Each later sub-step is detailed when it is next.

**3.1 Design** ([e2e-test-app.md](e2e-test-app.md))

- [x] Transit Sims in Doer terms: what is in the suites and what is not, tables, tasks and statuses, beans, REST API.
- [x] Specialized classes by aspect. Each case of *What Transit Sims must cover* gets a class, functional or specialized.
- [x] Each `*E2E` class of today: what it runs on in Transit Sims.
- [x] The open questions are decided or moved out of step 3.
- [x] Decided: bus steps of `1s`, a moving bus has the `path` to the next stop and `departed_at_ms`; Doer runs with the monitor.
- [x] **Check:** the design is reviewed and its status is `proposed`.

**3.2 Rename CarWash to Transit Sims**, without changing what any test checks

- [x] `e2e/carwash` → `e2e/transitsims`, package `carwash` → `transitsims`. `CarWashApplication` → `TransitSimsApplication`, `DoerResource` → `ValidationResource`, `CarWashProcess` → `TransitSimsProcess`. The `groupId` and `artifactId` of the generated projects change too.
- [x] The CarWash beans (`CarWash`, `Cafeteria`, `PhoneBooth`, `ExceptionMapper`, `Car`, `Shampoo`) move to `transitsims.validation` unchanged, until 3.3.
- [x] CarWash → Transit Sims in this document, in [src/test/README.md](src/test/README.md) and in the javadoc.
- [x] `DoerLifecycle` starts Doer with `start(true)`. `E2eEnvironment` stops Doer right after the first start of a node; a node restarted with `startNode` keeps Doer running. `reset` takes `m` as `start` does.
- [x] Doer: the idle monitor is woken up when a task is put into a queue (`reloadQueuedTask`), and computes its next check again. Before, a delayed task queued while Doer was idle waited for the monitor timeout (up to 60 s) instead of its delay.
- [x] **Check:** a test in `DoerServiceTest` (the scheduler without a database): Doer is idle with the monitor, a task with a delayed status is queued with `triggerTaskReloadFromDb`; the task runs after its delay, not after the timeout.
- [x] **Check:** `mvn verify` passes, and all `*E2E` classes pass with `-Ddoer.e2e.runtime=quarkus`. `git grep -i carwash -- src` finds nothing.
- [x] **Check:** `SmokeE2E.node_should_restart` runs on a running Doer: after `startNode`, a task with a delayed status is processed without `/api/validation/check`.

**3.3 Specialized classes for today's cases**

The CarWash beans (`Washer`, `Cafeteria`, `PhoneBooth`, `ExceptionMapper`, `Car`, `Shampoo`) and the demo doer methods, loaders and savers of `ValidationResource` are replaced by specialized classes in `transitsims.validation`, with statuses named after them. The `*E2E` classes switch to them without changing what they check. What no test uses goes (`Car is dusty`, `checkIn`).

- [x] `DoerMethodStatuses`: a method that hands the task over to `DoerMethodNextClass` (today `Want a coffee` → `Payed`), and a method with `delay = "2s"` (today `Receipt print started`). `ValidationResource` keeps `A` → `B` → `null` as `Validation resource first` → `Validation resource second` → `null`.
- [x] `ConcurrencyLimitOne`: `@ConcurrencyLimit(1)` on the class, two methods of 100 ms, one of them with two `@AcceptStatus` (today `PhoneBooth`).
- [x] `ConcurrencyQueues`: `@Dependent`, `@ConcurrencyLimit(10)` on the class. A method of 100 ms, a method that throws `Exception` without `@RetryPolicy`, and the chain class → method with `@ConcurrencyLimit(2)` → class (today `Cafeteria`). The slow and the failing method stay in one class: `queues__should_grow_and_shrink` checks the retry queue and the asap queue of one domain. The chain test moves from `DoerMethodsE2E` to `ConcurrencyE2E`.
- [x] `TransactionMethods` with the task data `TransactionData`: its loader and saver write `demo_log_tasks` with `txid_current()`, in the same bean as the doer methods. A method with `TransactionData`, the same method failing, and a method that calls `updateAndBumpVersion` (today `Need wash hands`). `coordinated_car_update` → `coordinated_data_update` with `TransactionData`.
- [x] `ErrorMethods`: a method that throws `Exception`, a method with `@RetryPolicy(interval = "2 sec", duration = "10 seconds", fallbackStatus)` that throws `RuntimeException`, and the describer of `RuntimeException`. `ErrorDescribers`: the describer of `Exception`.
- [x] `TransitSimsProcess.MAIN` injects the new beans. V2 is copied from the generated `CreateIndexes.sql` (the delayed status is new).
- [x] e2e-test-app.md: the *Code* column and the table of specialized classes name the new classes.
- [x] **Check:** `mvn verify` passes. All `*E2E` classes pass three runs in a row. `git grep -wE "Washer|Cafeteria|PhoneBooth|Shampoo|Car" -- src` finds nothing. Every filled-in row of the *Code* column in e2e-test-app.md points to a class of Transit Sims.

**3.4 The functional code of Transit Sims**

As in [e2e-test-app.md](e2e-test-app.md#transit-sims-in-doer-terms). The functional code is in the package `transitsims`.

- [x] `V4__create_transit_sims_tables.sql`: `sims` and `buses` with `id`, `task_id`, `created`, `modified`, `json_data`. `ValidationResource.reset` also deletes them. `SmokeE2E` expects V1–V4.
- [x] `Simulation` with its records and its calculation (nearest vertex, Dijkstra, path length, buses per route), and `Bus`; JSON-B with formatting. `SimStatus`, `BusStatus`.
- [x] `SimRepository` (loader, saver, `create` with `@Transactional`, find by id), `BusRepository` (loader with the simulation and `now()`, saver, find by simulation).
- [x] `SimSupervisor.checkCompletion`, `BusDriver.stand`, `drive`.
- [x] `SimResource`, `BusResource`: the endpoints of the REST API.
- [x] `TransitSimsProcess.MAIN` injects the new beans. V2 is copied from the generated `CreateIndexes.sql`.
- [x] `TransitSimsE2E`, on a line of three stops, Doer with the monitor (`reset?m=true`):
  - the simulation is created with its buses at their stops, and its JSON in `sims` is formatted;
  - buses go from stop to stop, with a `path` while driving;
  - concurrent boarding before the start: as many passengers board as the capacity allows, the others get 409; boarding at another stop gets 409;
  - a pause stops the simulation time and the buses, and a resume goes on;
  - a passenger rides from the first stop to the second and arrives; the bus parks at the terminal, and the simulation completes.
- [x] **Check:** `mvn verify` passes. All `*E2E` classes pass three runs in a row.

**3.5 Specialized classes for the missing cases**

- [ ] The cases marked `—` in e2e-test-app.md: kinds of beans, sources of statuses, retry forever, `@ConcurrencyGroup`, task data, exception describers, the external service on WireMock.
- [ ] **Check:** e2e-test-app.md has no `—` row, except the cases moved out of step 3.

**3.6 Lifecycle on Transit Sims**

- [ ] A node is stopped while tasks are in progress, and the restarted node finishes the simulation.
- [ ] Two nodes on one database run one simulation, and passengers board through both nodes.

**3.7 Done**

- [ ] e2e-test-app.md gets the status `implemented`, and its *Code* column is the only map of the cases.

### 4. Spring Boot

- [ ] `SpringBootApp`: `configureSpringBootApp` (`pom.xml` with `flyway-core` and `flyway-database-postgresql`, `application.properties`, `SpringBootApp.java`), `startSpringBootContainer`.
- [ ] All `*E2E` classes pass with `-Ddoer.e2e.runtime=spring-boot`; fixes in Doer or in the design, if needed.
- [ ] `spring-boot` in the CI matrix.

### 5. Jakarta EE servers: WildFly, Open Liberty

- [ ] `WildflyApp`, `OpenLibertyApp`; the shared `Producers.java` and `FlywayMigration.java` as constants in the test code.
- [ ] Both in the CI matrix.

### 6. Jetty

- [ ] Decide the open question about Jetty / Tomcat; then the same as step 5.

### 7. More build tools in `*GeneratedCodeITCase`

- [ ] `GradleGeneratedCodeITCase` (see [`*GeneratedCodeITCase`](#generatedcodeitcase-the-annotation-processor)); decide the open question about Gradle on CI.
- [ ] Eclipse compiler (ecj), if it is still a candidate.

### 8. More runtimes in the e2e suite

- [ ] Decide which candidates of the open question about supported runtimes (Payara, TomEE, Helidon MP, …) go into the matrix; each one as in step 5.

If, after step 5, several runtimes write the same Java files, they stay shared constants in the test code; a separate source folder only if text blocks become hard to maintain.

## Open questions

- **Which runtimes are "supported"** and go into the matrix? Proposed: Quarkus, Spring Boot, WildFly, Open Liberty, Jetty. Candidates: Payara, TomEE, Helidon MP. Micronaut is not Jakarta EE, but it would test whether its own annotation processor sees the generated service.
- **Jetty**: assembling CDI, JTA and a transactional `DataSource` by hand shows what a minimal setup needs, but it is the most text to keep in the test. Is that the setup we want, or is Tomcat with the same assembly more common?
- **Spring Boot with Jersey or with Spring MVC?** Jersey lets the REST endpoints stay in CarWash; most Spring users would use MVC.
- **Doer components in a dependency jar** (Quarkus needs a Jandex index, WARs need `beans.xml` in the jar): a second layout of CarWash, or out of scope?
- **Gradle on CI**: rely on the Gradle installed on the runner, or download a pinned distribution in the test?
