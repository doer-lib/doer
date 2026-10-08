package com.doer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * CarWash (src/test/resources/e2e/carwash), built with different build tools: the generated code compiles and works
 * the same way. {@link #MAIN} runs it without a container and a database. See e2e-design.md.
 */
public class CarWashITCase extends GeneratorTestBase {

    @Test
    void javac__should_build_working_car_wash() throws Exception {
        copySources("e2e/carwash", workspace.resolve("carwash"));
        writeSource("TestDoerService.java", TEST_DOER_SERVICE);
        writeSource("carwash/Main.java", MAIN);

        List<String> sources = javaFiles(workspace);
        javac(sources.toArray(String[]::new)).assertStatus(0);
        var result = java("carwash.Main").assertStatus(0);

        System.out.println("--------Generated code (just for reference)-------");
        System.out.println(generatedService());

        assertEquals(MAIN_OUT, result.stdOut());
    }

    @Test
    void maven__should_build_working_car_wash() throws Exception {
        Path srcRoot = workspace.resolve("src/main/java");
        copySources("e2e/carwash", srcRoot.resolve("carwash"));
        writeSource(srcRoot, "demo/test/TestDoerService.java", TEST_DOER_SERVICE);
        writeSource(srcRoot, "carwash/Main.java", MAIN);
        writeSource("src/test/java/carwash/DoerMethodInTests.java", DOER_METHOD_IN_TESTS);
        writeSource("pom.xml", POM.formatted(doerLibVersion, jakartaVersion, parssonVersion, doerLibVersion));

        var build = mvn(workspace, "mvn-package", "package").assertStatus(0);
        var result = mvn(workspace, "mvn-exec", "-q", "exec:java").assertStatus(0);

        assertEquals(MAIN_OUT, result.stdOut());
        assertTrue(Files.exists(workspace.resolve("target/test-classes/carwash/DoerMethodInTests.class")));
        assertTrue(build.stdOut().contains("The class _GeneratedDoerService is already present in dependencies"),
                "DoerProcessor did not notice that it compiles tests, see mvn-package-out.txt");
        assertFalse(Files.exists(workspace.resolve("target/test-classes/com/doer/generated")));
        assertFalse(Files.exists(workspace.resolve("target/generated-test-sources/test-annotations/com/doer")));
    }

    /** Project of CarWash; parameters: doer, Jakarta EE API, Parsson and doer (processor) versions. */
    static final String POM = """
            <?xml version="1.0" encoding="UTF-8"?>
            <project xmlns="http://maven.apache.org/POM/4.0.0"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
              <modelVersion>4.0.0</modelVersion>
              <groupId>carwash</groupId>
              <artifactId>carwash</artifactId>
              <version>1.0-SNAPSHOT</version>

              <properties>
                <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
                <maven.compiler.release>17</maven.compiler.release>
              </properties>

              <dependencies>
                <dependency>
                  <groupId>com.java-doer</groupId>
                  <artifactId>doer</artifactId>
                  <version>%s</version>
                </dependency>
                <dependency>
                  <groupId>jakarta.platform</groupId>
                  <artifactId>jakarta.jakartaee-api</artifactId>
                  <version>%s</version>
                </dependency>
                <dependency>
                  <groupId>org.eclipse.parsson</groupId>
                  <artifactId>parsson</artifactId>
                  <version>%s</version>
                  <scope>runtime</scope>
                </dependency>
              </dependencies>

              <build>
                <plugins>
                  <plugin>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.16.0</version>
                    <configuration>
                      <parameters>true</parameters>
                      <annotationProcessorPaths>
                        <path>
                          <groupId>com.java-doer</groupId>
                          <artifactId>doer</artifactId>
                          <version>%s</version>
                        </path>
                      </annotationProcessorPaths>
                    </configuration>
                  </plugin>
                  <plugin>
                    <groupId>org.codehaus.mojo</groupId>
                    <artifactId>exec-maven-plugin</artifactId>
                    <version>3.6.4</version>
                    <configuration>
                      <mainClass>carwash.Main</mainClass>
                    </configuration>
                  </plugin>
                </plugins>
              </build>
            </project>
            """;

    /**
     * A test source class with a doer method: DoerProcessor must see that the main classes already have
     * _GeneratedDoerService and generate nothing (DoerProcessor.isCompilingTests).
     */
    static final String DOER_METHOD_IN_TESTS = """
            package carwash;

            import com.doer.AcceptStatus;
            import com.doer.Task;

            public class DoerMethodInTests {
                @AcceptStatus("Test sources are compiled")
                public void doerMethod(Task task) {
                    task.setStatus(null);
                }
            }
            """;

    /** Paths of the .java files under the folder, relative to it. */
    static List<String> javaFiles(Path folder) throws Exception {
        try (Stream<Path> files = Files.walk(folder)) {
            return files.filter(f -> f.toString().endsWith(".java"))
                    .map(f -> folder.relativize(f).toString())
                    .sorted()
                    .toList();
        }
    }

    /**
     * Wires the CarWash components by hand and runs doer methods through {@code runTask}. CarWash and DoerResource
     * get a recording DataSource, which prints the executed statements instead of running them; the task log is
     * printed instead of written. Tasks are anonymous subclasses of Task, because {@code Task.setId} is protected.
     */
    static final String MAIN = """
            package carwash;

            import carwash.validation.DoerResource;
            import com.doer.Task;
            import demo.test.TestDoerService;
            import java.lang.reflect.Proxy;
            import java.sql.Connection;
            import java.sql.PreparedStatement;
            import java.time.Duration;
            import java.time.Instant;
            import java.util.ArrayList;
            import java.util.List;
            import javax.sql.DataSource;

            public class Main {
                public static void main(String[] args) throws Exception {
                    DataSource ds = recordingDataSource();
                    var carWash = new CarWash();
                    carWash.ds = ds;
                    var doerResource = new DoerResource();
                    doerResource.setDataSource(ds);
                    var doer = new TestDoerService() {
                        @Override
                        public long writeTaskLog(Long taskId, String initialStatus, String finalStatus,
                                String className, String methodName, String exceptionType, String extraJson,
                                Integer durationMs) {
                            System.out.println("  task log: " + className + "." + methodName
                                    + (exceptionType == null ? "" : ", " + exceptionType + ", " + extraJson));
                            return 1;
                        }
                    };
                    doerResource.setDoerService(doer);
                    doer._inject_cafeteria(new Cafeteria());
                    doer._inject_carWash(carWash);
                    doer._inject_exceptionMapper(new ExceptionMapper());
                    doer._inject_phoneBooth(new PhoneBooth());
                    doer._inject_doerResource(doerResource);

                    run(doer, task("Car is dusty"));
                    run(doer, task("Car need polishing"));
                    run(doer, task("Want a coffee"));
                    run(doer, task("Need order pizza"));
                    Task ab = task("A");
                    run(doer, ab);
                    run(doer, ab);
                    run(doer, task("Should send email"));
                    Task checkEmail = task("Should check email");
                    checkEmail.setFailingSince(Instant.now().minus(Duration.ofHours(1)));
                    run(doer, checkEmail);
                }

                static Task task(String status) {
                    Task task = new Task() {
                        {
                            setId(1L);
                        }
                    };
                    task.setStatus(status);
                    return task;
                }

                static void run(TestDoerService doer, Task task) throws Exception {
                    System.out.println(task.getStatus() + ":");
                    doer.runTask(task);
                    System.out.println("  -> " + task.getStatus() + (task.getFailingSince() == null ? "" : " (failing)"));
                }

                /** Prints each executeUpdate with the parameters in place of '?'. */
                static DataSource recordingDataSource() {
                    return proxy(DataSource.class, (method, args) -> switch (method) {
                        case "getConnection" -> proxy(Connection.class, (m, a) -> switch (m) {
                            case "prepareStatement" -> recordingStatement((String) a[0]);
                            case "close" -> null;
                            default -> throw new UnsupportedOperationException(m);
                        });
                        default -> throw new UnsupportedOperationException(method);
                    });
                }

                static PreparedStatement recordingStatement(String sql) {
                    List<Object> params = new ArrayList<>();
                    return proxy(PreparedStatement.class, (m, a) -> switch (m) {
                        case "setLong", "setBoolean" -> {
                            params.add(a[1]);
                            yield null;
                        }
                        case "executeUpdate" -> {
                            String s = sql;
                            for (Object p : params) {
                                s = s.replaceFirst("\\\\?", String.valueOf(p));
                            }
                            System.out.println("  sql: " + s);
                            yield 1;
                        }
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(m);
                    });
                }

                interface Handler {
                    Object handle(String method, Object[] args) throws Exception;
                }

                static <T> T proxy(Class<T> type, Handler handler) {
                    return type.cast(Proxy.newProxyInstance(Main.class.getClassLoader(), new Class<?>[] {type},
                            (p, method, args) -> handler.handle(method.getName(), args)));
                }
            }
            """;

    static final String MAIN_OUT = """
            Car is dusty:
              sql: insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', 1, true, txid_current());
              sql: insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Shampoo', 1, true, txid_current());
              sql: insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Shampoo', 1, false, txid_current());
              sql: insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', 1, false, txid_current());
              task log: CarWash.washTheCar
              -> Car is washed
            Car need polishing:
              sql: insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', 1, true, txid_current());
              sql: insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', 1, false, txid_current());
              task log: CarWash.polishTheCar
              -> Car is polished
            Want a coffee:
              task log: Cafeteria.visitCafeteria
              -> Customer is ready to pay
            Need order pizza:
              task log: PhoneBooth.makeACall
              -> Call finished
            A:
              task log: DoerResource.consumeTaskA
              -> B
            B:
              task log: DoerResource.consumeTaskB
              -> null
            Should send email:
              sql: insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', 1, true, txid_current());
              task log: Cafeteria.sendEmailToCustomer, java.lang.Exception, {
                "message": "Email sending failed",
                "e1": "Exception"
            }
              -> Should send email (failing)
            Should check email:
              sql: insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', 1, true, txid_current());
              task log: Cafeteria.checkEmail, java.lang.RuntimeException, {
                "message": "Check email failed",
                "e1": "Exception",
                "e2": "RuntimeException"
            }
              -> Email check failed
            """;
}
