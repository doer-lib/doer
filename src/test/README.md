# Doer tests

## Kinds of tests

Each kind of test has its own system under test (SUT). The package of a test class tells its kind. The suffix of its name tells who runs it.

| # | Kind | System under test | Examples | Package |
|---|---|---|---|---|
| 1 | Unit | Doer classes in the test JVM, without the annotation processor | `DoerServiceTest` | `com.doer` |
| 2 | SQL | Doer's SQL on Postgres. The Java code runs in the test JVM. The SQL comes from the annotation processor. | `DoerServiceJdbcITCase` | `com.doer` |
| 3 | Annotation processing | javac + `DoerProcessor`. The test data is Java code. The results are javac messages, the files in `com/doer/generated`, or the output of the compiled code run with `java`. | `GeneratorITCase`, `GeneratorErrorsITCase` | `com.doer.processor` |
| 4 | Generated code, build tools | Transit Sims built with the processor by a build tool, running in its own JVM. Nobody serves the Jakarta EE annotations: `Main` wires the beans by hand. | `JavacGeneratedCodeITCase`, `MavenGeneratedCodeITCase` | `com.doer.generatedcode` |
| 5 | Generated code, runtimes | The same checks as 4, on Transit Sims deployed in a runtime (Quarkus, …) | `GeneratedCodeE2E` | `com.doer.generatedcode` |
| 6 | Runtime functional | The generated code and the user's code in a runtime. This checks how the runtime serves the Jakarta EE annotations: CDI, transactions, REST, shutdown. | `TransactionsE2E`, `ConcurrencyE2E` | `com.doer.e2e` |

Kind 3 checks one feature or edge case of the processor per test, each on its own small sources. Kinds 4 and 5 check what `runTask` of the generated service does, on Transit Sims, the test application in [`resources/e2e/transitsims`](resources/e2e/transitsims) (see [e2e-test-app.md](../../e2e-test-app.md)).

## Where the tests run

| Suffix | Runner | Where in CI |
|---|---|---|
| `*Test` | surefire, `mvn test` | every JDK of the matrix (17, 21, 25, 27) |
| `*ITCase` | failsafe, `mvn verify`, one JVM per class | every JDK of the matrix. Each build tool of kind 4 is a class of its own. |
| `*E2E` | failsafe in the `e2e` profile, `mvn verify -Ddoer.e2e.runtime=<runtime>`, one JVM for all classes | each runtime with its recommended JDK (see [e2e-design.md](../../e2e-design.md)) |

Kinds 4 and 5 share the tests and differ only in the SUT. The checks are `@Test` default methods of the interface [`GeneratedCodeTest`](java/com/doer/generatedcode/GeneratedCodeTest.java). Each implementing class builds and starts Transit Sims its own way and implements `runTask(request)`.

```
GeneratedCodeTest                the checks: request → expected response of transitsims.validation.TaskRunner
├── JavacGeneratedCodeITCase     javac       → TransitSimsProcess: java transitsims.Main, JSON Lines over stdin/stdout
├── MavenGeneratedCodeITCase     mvn package → TransitSimsProcess
└── GeneratedCodeE2E             E2eEnvironment: deployed in Docker → POST /api/validation/run-task
```

A new build tool or a new runtime adds an implementation, not tests.

## How the tests are built

The tests have no base classes for infrastructure. [`com.doer.testkit`](java/com/doer/testkit) has static helpers:

- `Toolchain`: `javac`, `java` and `mvn` in a folder; the classpath from the local Maven repository.
- `Sources`: writes sources from text blocks and copies them from test resources; `TEST_DOER_SERVICE`.
- `Processes`: runs a command and keeps its command line, stdout and stderr in the folder.
- `Postgres`, `Sql`: the Postgres container of kind 2; one-line SQL.

`InWorkspace` binds these helpers to the folder of the test through `Path getWorkspace()`. A test that implements it calls `writeSource(...)`, `javac(...)`, `java(...)`, `mvn(...)`, `generated(...)` and `doerJson()` in its workspace. JUnit creates the folder with `@TempDir`, and `Workspaces` gives it a stable name:

```java
public class GeneratorITCase implements InWorkspace {      // a workspace per test method

    @TempDir(factory = Workspaces.class, cleanup = NEVER)
    Path workspace;

    @Override
    public Path getWorkspace() {
        return workspace;
    }
```

A class that builds once for all its tests, as kind 4 does, makes the `@TempDir` field static, because JUnit sets static fields before `@BeforeAll` and instance fields only before each test. The class also uses `@TestInstance(PER_CLASS)`. Then `@BeforeAll` is an instance method that can call `javac(...)`, and the running SUT is an instance field.

The workspaces are cleared before a test and kept after it. After a failure they have the sources, the compiled and generated files, and the output of every command:

```
target/it-test-workspaces/<class>/            per class (a static @TempDir field)
target/it-test-workspaces/<class>/<method>/   per test method
target/it-test-workspaces/_dependencies/      mvn dependency:get of the Jakarta EE API and Parsson
target/e2e/<runtime>/                         the e2e application: a complete Maven project, docker and app logs
```

The ITCase and E2E classes use the doer jar of the current build, `0.0.0-IT-SNAPSHOT`. `mvn verify` installs it in `pre-integration-test`. To run a single ITCase or E2E class from the IDE, first run `mvn -DskipTests verify`.
