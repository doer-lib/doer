# End-to-end functional testing

Status: proposed

Doer is tested by two questions that need different test matrices:

1. **Does the annotation processor generate correct code with every supported JDK and build tool?** The answer does not depend on the runtime the application is deployed to. It needs the JDK matrix (17, 21, 25, 27) and several build tools, but no database and no container.
2. **Does the generated code work in a real application in every supported runtime?** The answer barely depends on the JDK. It needs a database, external services and a running application in Quarkus, Spring Boot, WildFly and others, but only one JDK.

Both questions are asked about the same code: **CarWash**, a Jakarta EE backend application that uses Doer.

| Suite | What it checks | Matrix | How it uses CarWash |
|---|---|---|---|
| `CarWashITCase` | the annotation processor | JDKs × build tools (javac, Maven, Gradle) | adds its own `Main`, runs key parts of Doer without a database |
| e2e suite (`*E2E` classes) | the generated code in a deployed application | runtimes (`quarkus`, `spring-boot`, `wildfly`, …), one JDK | adds DB migrations and a runtime setup, runs it in Docker, tests it over REST, JDBC and WireMock |

This document describes only the infrastructure: the CarWash project, how each suite builds and runs it, and CI. What CarWash does and what exactly the suites check will be designed in a separate document, once the infrastructure is in place.

`GeneratorITCase`, `GeneratorErrorsITCase`, `DoerServiceJdbcITCase` and the unit tests stay as they are, in the JDK matrix.

## Problems today

- **Only Quarkus is tested.** Nothing tells whether the generated service works where CDI, JTA or the `DataSource` behave differently: WildFly, Open Liberty, Spring Boot.
- **`QuarkusITCase` runs in every JDK of the matrix**, so the slowest test is built and run 4 times, although it tests the runtime, not the JDK.
- **The application is assembled in a fragile way.** `QuarkusITCase` generates a project with `quarkus:create` on every run, patches its `pom.xml` with regular expressions, and builds it twice to get the Flyway script.
- **The processor and the runtime are tested on different code.** `CarWashITCase` has its own toy classes in text blocks, and `QuarkusITCase` uses `src/test/resources/e2e-code`. Code that compiles and runs in `CarWashITCase` is not the code that runs in Quarkus.

## Overview

```
                    src/test/resources/e2e/carwash   (CarWash: com.doer + jakarta.* APIs only)
                                 │
         ┌───────────────────────┴──────────────────────────┐
         │                                                  │
 CarWashITCase                              E2eEnvironment, -Ddoer.e2e.runtime=<runtime>
 + Main (text block)                        copySources → configure<Runtime>App → mvn package
 + TestDoerService (no DB)                  → check migrations → docker run --rm (Flyway)
 javac / Maven / Gradle                                     │
 × JDK 17, 21, 25, 27                   ┌──────────── Docker network ─────────────┐
                                        │  CarWash (1+ nodes)  postgres  wiremock │
                                        └─────▲──────────────────▲─────────▲──────┘
                                              │ REST             │ JDBC    │ admin API
                                          e2e test classes (*E2E), one JDK
```

## CarWash

CarWash is a Jakarta EE backend application that keeps the records of a car wash, with Doer running its business operations. Its functionality will be defined later. For the infrastructure, what matters is that it is **one application with a set of components of every kind a Doer user writes**, including REST endpoints.

### Components

| Kind | Purpose |
|---|---|
| CDI beans (`@ApplicationScoped`, `@Dependent`, …) with doer methods | business operations: `@AcceptStatus`, `@RetryPolicy`, `@ConcurrencyLimit`, `@ConcurrencyGroup` |
| `@TaskDataLoader`, `@TaskDataSaver`, `@ExceptionDescriber` methods | in the same beans as the doer methods and in other beans |
| **functional REST endpoints** (JAX-RS) | the API of CarWash for its clients; start business operations, coordinated updates (`facilitateCoordinatedUpdate`) |
| **validation REST endpoints** (JAX-RS) | for the tests only: control Doer (start, stop, reload, check ready tasks, reset stalled tasks), read tasks, reset data, report the runtime the application runs in. Today's `DoerResource` is the first version of them |
| repositories | JDBC through the injected `DataSource` |
| clients of external services | JAX-RS client API (`jakarta.ws.rs.client`); in e2e the services are simulated by WireMock |
| `DoerLifecycle` | `@Observes Startup` → `doerService.start(false)`, `@Observes Shutdown` → `stop()` |
| `CarWashConfig` | URLs of external services from environment variables (`System.getenv`); MicroProfile Config is missing in Spring Boot and Jetty |

All endpoints, functional and validation, are in one application under `@ApplicationPath("/api")`; the validation endpoints have their own path, `/api/validation`.

### The one rule

**CarWash imports only `com.doer`, Jakarta Web Profile APIs (CDI, JTA, JAX-RS, JSON-P) and `javax.sql`.** Nothing of Quarkus, Spring or WildFly. Then the same sources compile in `CarWashITCase` and run in every runtime.

Today's `DoerResource` breaks the rule with `io.quarkus.runtime.StartupEvent`; it is replaced by the CDI 4 `jakarta.enterprise.event.Startup` event (Jakarta EE 10), which Quarkus, WildFly and Open Liberty fire.

So that `CarWashITCase` can run CarWash without a container, components get their collaborators through `@Inject` setters or package-private fields, and external services and repositories are behind small interfaces that `Main` can replace with in-memory implementations. `Main` is in the package `carwash`, so components of other packages (`carwash.validation`) need `@Inject` setters. Components that use the `DataSource` directly (today the loaders and savers that write `demo_log_tasks`) get a recording `DataSource` from `Main`: a `java.lang.reflect.Proxy` without a database that prints each statement it executes.

### Layout

CarWash stays in test resources. `e2e/carwash` is the folder of the Java package `carwash`: it is copied to `<workdir>/src/main/java/carwash` as it is. DB migrations are separate: only the e2e suite uses them; they are copied to `<workdir>/src/main/resources/db/migration`, the default location of Flyway.

```
src/test/resources/e2e/
  carwash/                          package carwash: components, functional REST endpoints, @ApplicationPath("/api") → <workdir>/src/main/java/carwash
    validation/                     package carwash.validation: validation REST endpoints (/api/validation)
  db/migration/
    V1__create_doer_schema.sql        = generated CreateSchema.sql: Doer tables and sequences
    V2__create_doer_indexes.sql       = generated CreateIndexes.sql: Doer indexes for CarWash statuses
    V3__create_validation_tables.sql  tables and triggers needed by the tests
```

CarWash has no tests of its own: the suites test CarWash, not tests inside it.

`V1` and `V2` are what a Doer user would commit: copies of `CreateSchema.sql` and `CreateIndexes.sql`, one file each, as the processor generates them for CarWash. One file per generated file keeps them comparable as they are. The e2e suite compares each of them with its generated file in the work folder, fails on a difference and names the generated file to copy over it; so a change of CarWash that changes the schema (a new status, a new concurrency group) also changes the migration in the same commit. The application sets up the database itself: it runs these migrations with Flyway at start, before Doer starts (see [What each runtime writes](#what-each-runtime-writes)).

`src/test/resources/e2e-code` is moved here: its Java code becomes the first version of CarWash, its trigger SQL `V3__create_validation_tables.sql`.

**No shared glue folders**, at least at first. What a runtime cannot work without — `pom.xml`, configuration, a `SpringBootApp` class, a `DataSource` producer — is written by that runtime's methods in the test code, as text blocks (see [Runtime setup](#runtime-setup)). When two runtimes need the same text (the producers of WildFly and Open Liberty), it is a shared constant in the test code, not a folder.

## `CarWashITCase`: the annotation processor

`CarWashITCase` copies `src/test/resources/e2e/carwash` into the `carwash` package folder of its workspace and compiles it together with two text blocks of its own, as today:

- `TEST_DOER_SERVICE` — the generated service without a database;
- `Main` — wires the CarWash components by hand (the `_inject_*` methods of the generated service, in-memory implementations instead of repositories and external services), runs key parts of Doer through `runTask` and prints what is called. The expected output is a text block next to it.

CarWash is compiled against the Jakarta EE API jar, which `GeneratorTestBase` already resolves. The REST endpoints and JDBC repositories are compiled but not run by `Main`. No database is needed.

Each build tool is a test method:

| Build tool | How |
|---|---|
| javac | as today: `GeneratorTestBase.javac` |
| Maven | `pom.xml` as a text block (instead of the archetype), sources copied in, processor in `annotationProcessorPaths` |
| Gradle | new: `build.gradle` as a text block with `annotationProcessor "com.java-doer:doer:…"` and `mavenLocal()`; a pinned Gradle version |
| Eclipse compiler (ecj) | candidate: the compiler of Eclipse and VS Code Java; runs processors differently from javac |

The Maven and Gradle projects also have a test source folder with one class that has a doer method. The build compiles it with the processor on the path, and the check is that the processor changes nothing: no second `_GeneratedDoerService`, and `CreateSchema.sql`, `doer.json` and the other generated files of the main classes stay as they are (`DoerProcessor.isCompilingTests`; today's `DemoTest` in `QuarkusITCase`).

`CarWashITCase` stays in the JDK matrix, so it runs with JDK 17, 21, 25 and 27.

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

Each runtime is one class in `src/test/java/com/doer/e2e/` that turns the CarWash sources into a Maven project and says how to run its artifact in Docker:

```java
class QuarkusApp implements E2eApp {
    @Override
    public void configure(Path workdir) throws Exception {
        copySources("e2e/carwash", workdir.resolve("src/main/java/carwash"));
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
- **The work folder `target/e2e/<runtime>/` is a complete Maven project** after `configure`. To debug, open it in the IDE, run it, and point the tests at it with `doer.e2e.runtime=external`. Fixes are copied back to `src/test/resources/e2e/carwash` by hand.
- `mvn package` in the work folder is the same for all runtimes and uses the local repository of the outer build (`-Dmaven.repo.local` from `doer.test.m2.repo`).
- **Doer comes in as `0.0.0-IT-SNAPSHOT`** (`doer.lib.version`), the jar of the current build: the execution `install-doer-for-it` of the outer `pom.xml` installs it into the local repository in `pre-integration-test`, before the e2e suite runs. An e2e class run from the IDE uses what the last `mvn verify` (or `mvn -DskipTests verify`) installed.

### What each runtime writes

The generated `_GeneratedDoerService` needs a `DataSource` bean, an `Executor` bean, `@jakarta.transaction.Transactional` interceptors and CDI-style injection. CarWash needs JAX-RS and the `Startup` / `Shutdown` events. The database must be migrated by Flyway before Doer starts. Where the runtime gives them, nothing is written; otherwise the runtime's `configure…App` writes the smallest file that fills the gap:

| Runtime | Provided by the runtime | Written by the test |
|---|---|---|
| **quarkus** | `DataSource` (Agroal), `Executor`, JTA (Narayana), CDI events, RESTEasy, Flyway (`quarkus-flyway`, `migrate-at-start`) | `pom.xml`, `application.properties` |
| **wildfly** | JTA, CDI events, RESTEasy | `pom.xml` (WAR), `WEB-INF/carwash-ds.xml` with `${env.E2E_DB_URL}`, `Producers.java`: `DataSource` from `@Resource(lookup)`, `Executor` from `ManagedExecutorService`; `FlywayMigration.java` |
| **open-liberty** | JTA, CDI events, JAX-RS | `pom.xml` (WAR), `server.xml` (features, data source, Postgres driver), the same `Producers.java` and `FlywayMigration.java` |
| **spring-boot** | `DataSource` (Hikari), transactions (Spring reads the jakarta annotation), Flyway (auto-configured with `flyway-core`) | `pom.xml`, `application.properties`, `SpringBootApp.java` (see below) |
| **jetty** | servlet container only | `pom.xml`, Weld servlet, Jersey and Narayana setup, `web.xml`, producers of a transactional `DataSource` and an `Executor`, `FlywayMigration.java` |

`FlywayMigration.java`, for the runtimes without a Flyway integration, observes `Startup` with a `@Priority` lower than the one of `DoerLifecycle`, so that CDI calls it first, and runs `Flyway.configure().dataSource(ds).load().migrate()`. Flyway is a dependency of the runtime's `pom.xml`, not of CarWash: CarWash does not import `org.flywaydb`. In Quarkus and Spring Boot the migration runs before the `Startup` event / `ApplicationReadyEvent`, so nothing is written. Every runtime also needs `flyway-database-postgresql`.

`SpringBootApp.java` carries everything Spring does differently from CDI, so that CarWash stays free of Spring:

- `@Import(_GeneratedDoerService.class)`, because Spring does not know `@ApplicationScoped`;
- a component scan of `carwash` with an include filter on CDI scope annotations, and a `ScopeMetadataResolver` that maps `@ApplicationScoped` → singleton, `@Dependent` → prototype;
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
   docker run --rm --name carwash-<runtime>-<node>-<start> --network <network> -p 127.0.0.1:<port>:8080
       --label org.testcontainers.sessionId=<session>
       -e E2E_RUNTIME=<runtime> -e E2E_DB_URL=jdbc:postgresql://db:5432/doer -e E2E_DB_USER=… -e E2E_DB_PASSWORD=…
       <options> <image> <command> of E2eApp.dockerRun
   ```
   with stdout and stderr appended to `target/e2e/<runtime>/app-<node>-out.txt` and `app-<node>-err.txt`, and the command written to `app-<node>-cmd.txt`. The host port is picked once per node and kept across restarts. The URLs of the external services on WireMock are added as `-e` when CarWash gets them. The image is pulled with `docker pull` before, so that the wait below does not include the download;
6. waits until the validation endpoint `GET /api/validation/info` answers with the expected runtime (so the database is migrated and Doer is started); on a timeout, or when the process exits before, it fails with the tail of the log.

**Restarts.** `stopNode(node)` calls `process.destroy()`: SIGTERM to `docker run`, which passes it to the application in the container (`--sig-proxy` is on by default). Then `waitFor`, and both log files get a separator line with the timestamp, how long the stop took and the exit code. `--rm` removes the container. `startNode(node)` appends a separator line with the timestamp and runs `docker run` again into the same files. The container name carries the start number, because Docker removes the old container a moment after `docker run` exits. More nodes on the same database are started the same way, each with its own port and log files.

**Cleanup.** The application containers carry the Testcontainers session label, so Ryuk removes them, with Postgres, WireMock and the network, when the test JVM ends, even if it crashes; a shutdown hook also stops the running processes. The `docker` CLI must be on `PATH`.

Tests get three handles from it:

| Handle | Gives access to |
|---|---|
| **REST** (RestAssured, base URI of node 1) | functional and validation endpoints |
| **JDBC** (`DataSource` to the mapped Postgres port) | `tasks`, `task_logs` and CarWash tables |
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

- **One JDK** (25, the latest LTS) for building CarWash and in the container image. A runtime that lags behind can stay on 21 with an `include` entry in the matrix and a JDK parameter of its `E2eApp`.
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

javac and Maven only; more build tools are [step 7](#7-more-build-tools-in-carwashitcase).

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

### 3. CarWash

- [ ] A separate document that develops CarWash: its functionality, what `CarWashITCase` checks about the generated code, and what the e2e suite checks about its behavior in the runtimes.

### 4. Spring Boot

- [ ] `SpringBootApp`: `configureSpringBootApp` (`pom.xml` with `flyway-core` and `flyway-database-postgresql`, `application.properties`, `SpringBootApp.java`), `startSpringBootContainer`.
- [ ] All `*E2E` classes pass with `-Ddoer.e2e.runtime=spring-boot`; fixes in Doer or in the design, if needed.
- [ ] `spring-boot` in the CI matrix.

### 5. Jakarta EE servers: WildFly, Open Liberty

- [ ] `WildflyApp`, `OpenLibertyApp`; the shared `Producers.java` and `FlywayMigration.java` as constants in the test code.
- [ ] Both in the CI matrix.

### 6. Jetty

- [ ] Decide the open question about Jetty / Tomcat; then the same as step 5.

### 7. More build tools in `CarWashITCase`

- [ ] Gradle (see [`CarWashITCase`](#carwashitcase-the-annotation-processor)), with the same test source class and check as Maven; decide the open question about Gradle on CI.
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
