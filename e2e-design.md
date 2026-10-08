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
 + TestDoerService (no DB)                  → check migrations → start<Runtime>Container (Flyway)
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

So that `CarWashITCase` can run CarWash without a container, components get their collaborators through `@Inject` setters or package-private fields, and external services and repositories are behind small interfaces that `Main` can replace with in-memory implementations.

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

Each runtime is one class in `src/test/java/com/doer/e2e/` that turns the CarWash sources into a running container:

```java
class QuarkusApp implements E2eApp {
    @Override
    public void configure(Path workdir) throws Exception {
        copySources("e2e/carwash", workdir.resolve("src/main/java/carwash"));
        copySources("e2e/db/migration", workdir.resolve("src/main/resources/db/migration"));
        configureQuarkusApp(workdir);
    }

    @Override
    public GenericContainer<?> startContainer(Path workdir, Network network, int node) {
        return startQuarkusContainer(workdir, network, node);
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

    GenericContainer<?> startQuarkusContainer(Path workdir, Network network, int node) {
        var container = new GenericContainer<>("eclipse-temurin:25-jre")
                .withCopyFileToContainer(MountableFile.forHostPath(workdir.resolve("target/quarkus-app")), "/app")
                .withCommand("java", "-jar", "/app/quarkus-run.jar")
                ...;
        container.start();
        return container;
    }
}
```

- **The whole `pom.xml` is a text block**, not an archetype patched with regular expressions. The versions of the runtime and of Doer are its only parameters.
- **No Dockerfiles.** The container is the runtime's official image (or a JRE image) with the built artifact copied in by Testcontainers (`withCopyFileToContainer`): `quarkus-app/` to a JRE image, `ROOT.war` to WildFly's `standalone/deployments`, `ROOT.war` and `server.xml` to Open Liberty's `config/`.
- **The work folder `target/e2e/<runtime>/` is a complete Maven project** after `configure`. To debug, open it in the IDE, run it, and point the tests at it with `doer.e2e.runtime=external`. Fixes are copied back to `src/test/resources/e2e/carwash` by hand.
- `mvn package` in the work folder is the same for all runtimes and uses the local repository of the outer build (`GeneratorTestBase.mvn` already does this).

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

1. starts a Testcontainers `Network`, Postgres (alias `db`) and WireMock (alias `wiremock`);
2. picks the `E2eApp` for `doer.e2e.runtime` and calls `configure(target/e2e/<runtime>)`;
3. runs `mvn -B package` in the work folder;
4. checks that `V1__create_doer_schema.sql` equals `CreateSchema.sql` and `V2__create_doer_indexes.sql` equals `CreateIndexes.sql`, generated in `target/classes/com/doer/generated` of the work folder; on a difference it fails and names the generated file to copy. It does not touch the database: the application migrates it with Flyway at start. Because the generated SQL is committed, the double build of `QuarkusITCase` goes away;
5. calls `startContainer` with `E2E_RUNTIME`, `E2E_DB_URL`, `E2E_DB_USER`, `E2E_DB_PASSWORD` and the URLs of the external services on WireMock;
6. waits until the validation endpoint `GET /api/validation/info` answers with the expected runtime (so the database is migrated and Doer is started), and writes the container log to `target/e2e/<runtime>/app-<node>.log` as it comes.

Tests get three handles from it:

| Handle | Gives access to |
|---|---|
| **REST** (RestAssured, base URI of node 1) | functional and validation endpoints |
| **JDBC** (`DataSource` to the mapped Postgres port) | `tasks`, `task_logs` and CarWash tables |
| **WireMock** (client on the mapped admin port) | stubs and received requests of the external services |

Plus `appLog(node)` for the application log, and `killNode` / `startNode` to stop and start application containers and run more than one node on the same database.

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
        uses: actions/upload-artifact@v4
        with:
          name: e2e-${{ matrix.runtime }}
          path: target/e2e/
```

- **One JDK** (25, the latest LTS) for building CarWash and in the container image. A runtime that lags behind can stay on 21 with an `include` entry in the matrix and a JDK parameter of its `E2eApp`.
- **Runtime versions** are constants in the runtime's `E2eApp` class. Testing two versions of one runtime (e.g. a Jakarta EE 10 and an EE 11 WildFly) is a matrix `include` with `-Ddoer.e2e.runtime.version=…`, added only when needed.
- The Maven repository cache gets the runtime in its key: Quarkus, Spring Boot and WildFly bring different dependencies.
- The uploaded `target/e2e/<runtime>/` has both the application logs and the generated project, so a failure can be reproduced from the artifact.

## Implementation plan

The plan is worked through top to bottom. A box is ticked when its **Check** passes. When the work shows that the plan or the design is wrong, fix the design first, then the plan, and go on. Only step 1 is detailed; each later step is detailed when it is next.

### 1. Infrastructure on Quarkus

**1.1 CarWash sources** (`src/test/resources/e2e`)

- [ ] Move `e2e-code/tst/demo/*.java` to `e2e/carwash/`, package `tst.demo` → `carwash`.
- [ ] Move `DoerResource` to `e2e/carwash/validation/` (package `carwash.validation`), path `/doer` → `/validation`; add `CarWashApplication` with `@ApplicationPath("/api")`.
- [ ] Add `GET /api/validation/info`: `{"runtime": "<E2E_RUNTIME>"}`.
- [ ] Move start / stop of Doer from `DoerResource` to `DoerLifecycle`, with `jakarta.enterprise.event.Startup` instead of `io.quarkus.runtime.StartupEvent`.
- [ ] Replace `org.slf4j.Logger` in `CarWash` with `java.lang.System.Logger` (slf4j is not a Jakarta API).
- [ ] Delete `e2e-code/test/tst/demo/DemoTest.java`: CarWash has no tests of its own.
- [ ] `e2e/db/migration/V1__create_doer_schema.sql` and `V2__create_doer_indexes.sql`: copies of the generated `CreateSchema.sql` and `CreateIndexes.sql` of CarWash (replace the empty `V20240224_00__create_doer_tables.sql`).
- [ ] `e2e/db/migration/V3__create_validation_tables.sql`: moved from `V20240224_01__create_e2e_test_triggers.sql`.
- [ ] **Check:** `grep -rE "import (io\.quarkus|org\.springframework|org\.slf4j|org\.jboss)" src/test/resources/e2e` finds nothing.

**1.2 Maven**

- [ ] Profile `e2e` in `pom.xml`, activated by the property `doer.e2e.runtime`: a failsafe execution with `<includes>**/*E2E.java</includes>`, `reuseForks=true` and the system properties `doer.e2e.runtime`, `doer.lib.version`, `doer.test.m2.repo`; surefire and the default failsafe execution skipped.
- [ ] Test dependency of the WireMock client (`org.wiremock:wiremock-standalone`).
- [ ] **Check:** `mvn verify` runs no `*E2E` class; `mvn verify -Ddoer.e2e.runtime=quarkus` runs only `*E2E` classes (with a placeholder test).

**1.3 `E2eEnvironment`**

- [ ] `copySources(String resourceFolder, Path target)`: copies a folder of test resources; `writeFile` reuses `GeneratorTestBase.writeSource`.
- [ ] `E2eApp` interface: `configure(Path workdir)`, `startContainer(Path workdir, Network network, int node)`; `mvn -B package` in the work folder is common code (as `GeneratorTestBase.mvn`, with the outer local repository).
- [ ] `E2eEnvironment`, a JUnit extension with one instance per JVM: `Network`; its own Postgres container with alias `db` (`Utils.getPostgresDataSource()` has no network); WireMock with alias `wiremock` (no test uses it yet); work folder `target/e2e/<runtime>`.
- [ ] Checks that `V1__create_doer_schema.sql` equals the generated `CreateSchema.sql` and `V2__create_doer_indexes.sql` equals `CreateIndexes.sql` (on a difference: fails and names the generated file). The database is left to the application.
- [ ] Waits for `GET /api/validation/info` with the expected runtime; writes the container log to `target/e2e/<runtime>/app-1.log`.
- [ ] Handles: RestAssured base URI, `DataSource` of the e2e Postgres, WireMock client, `appLog(node)`.
- [ ] Properties: `doer.e2e.skip-build`; runtime `external` with `doer.e2e.base-url`, `doer.e2e.jdbc-url`, `doer.e2e.wiremock-url`; `quarkus` when the property is not set.

**1.4 `QuarkusApp`**

- [ ] `configureQuarkusApp`: `pom.xml` as a text block (Quarkus BOM, `quarkus-resteasy`, `quarkus-resteasy-jsonb`, `quarkus-jdbc-postgresql`, `quarkus-flyway`, `flyway-database-postgresql`, `doer` as a dependency and in `annotationProcessorPaths`, `quarkus-maven-plugin`); `e2e/db/migration` copied to `src/main/resources/db/migration`; `application.properties` with `${E2E_DB_URL}`, `${E2E_DB_USER}`, `${E2E_DB_PASSWORD}`, `quarkus.flyway.migrate-at-start=true`, port 8080, the log format of `QuarkusITCase`.
- [ ] `startQuarkusContainer`: `eclipse-temurin:25-jre`, `target/quarkus-app` copied to `/app`, environment variables, port 8080.
- [ ] **Check:** `SmokeE2E` (`GET /api/validation/info` answers `quarkus`; `flyway_schema_history` has V1, V2, V3) passes with `mvn verify -Ddoer.e2e.runtime=quarkus`, and again with `-Ddoer.e2e.skip-build=true`.
- [ ] **Check:** `target/e2e/quarkus` opens in the IDE as a Maven project, and `SmokeE2E` passes against it with `doer.e2e.runtime=external`.

**1.5 Tests of `QuarkusITCase`**

- [ ] Move the tests to a few `*E2E` classes by topic, without changing what they check; the REST and JDBC helpers (`pushTask`, `waitTaskStatus`, `restGetTask`, `loadDemoLogs`, …) to a shared class; JDBC through `E2eEnvironment` instead of `Utils`.
- [ ] Paths `doer/...` → `/api/validation/...`; `quarkus__should_start` → `SmokeE2E` (no SmallRye Health).
- [ ] `queues__should_grow_and_shrink` reads `appLog(1)` instead of `quarkus-app-out.txt`.
- [ ] Timing bounds widened only where Docker needs it, each change noted in the commit message.
- [ ] **Check:** all `*E2E` classes pass with `-Ddoer.e2e.runtime=quarkus` three runs in a row.
- [ ] Delete `QuarkusITCase` and `src/test/resources/e2e-code`.

**1.6 CI**

- [ ] Job `e2e` in `maven.yaml` with `runtime: [ quarkus ]`, as in [CI](#ci).
- [ ] **Check:** the `build` job no longer builds Quarkus; the `e2e` job is green; on a failure, `target/e2e/` is uploaded.

### 2. `CarWashITCase` on the CarWash sources

- [ ] `CarWashITCase` copies `e2e/carwash`, adds `TEST_DOER_SERVICE` and `Main`; the toy classes in text blocks are removed.
- [ ] javac and Maven (`pom.xml` as a text block instead of the archetype).
- [ ] Gradle.

### 3. Spring Boot

- [ ] `SpringBootApp`: `configureSpringBootApp` (`pom.xml` with `flyway-core` and `flyway-database-postgresql`, `application.properties`, `SpringBootApp.java`), `startSpringBootContainer`.
- [ ] All `*E2E` classes pass with `-Ddoer.e2e.runtime=spring-boot`; fixes in Doer or in the design, if needed.
- [ ] `spring-boot` in the CI matrix.

### 4. Jakarta EE servers: WildFly, Open Liberty

- [ ] `WildflyApp`, `OpenLibertyApp`; the shared `Producers.java` and `FlywayMigration.java` as constants in the test code.
- [ ] Both in the CI matrix.

### 5. Jetty

- [ ] Decide the open question about Jetty / Tomcat; then the same as step 4.

### 6. CarWash

- [ ] A separate document that develops CarWash: its functionality, what `CarWashITCase` checks about the generated code, and what the e2e suite checks about its behavior in the runtimes.

If, after step 4, several runtimes write the same Java files, they stay shared constants in the test code; a separate source folder only if text blocks become hard to maintain.

## Open questions

- **Which runtimes are "supported"** and go into the matrix? Proposed: Quarkus, Spring Boot, WildFly, Open Liberty, Jetty. Candidates: Payara, TomEE, Helidon MP. Micronaut is not Jakarta EE, but it would test whether its own annotation processor sees the generated service.
- **Jetty**: assembling CDI, JTA and a transactional `DataSource` by hand shows what a minimal setup needs, but it is the most text to keep in the test. Is that the setup we want, or is Tomcat with the same assembly more common?
- **Spring Boot with Jersey or with Spring MVC?** Jersey lets the REST endpoints stay in CarWash; most Spring users would use MVC.
- **Doer components in a dependency jar** (Quarkus needs a Jandex index, WARs need `beans.xml` in the jar): a second layout of CarWash, or out of scope?
- **Gradle on CI**: rely on the Gradle installed on the runner, or download a pinned distribution in the test?
