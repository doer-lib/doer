package com.doer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The car wash service, built with different build tools: the generated code compiles and works the same way. */
public class CarWashITCase extends GeneratorTestBase {

    @Test
    void javac__should_build_working_car_wash() throws Exception {
        writeCarWashClasses(workspace);
        writeSource("TestDoerService.java", TEST_DOER_SERVICE);
        String expectedOut = writeCarWashMain(workspace);

        javac("Soap.java",
                "Driver.java",
                "Car.java",
                "test2/Car.java",
                "Washer.java",
                "CarWash.java",
                "Bar.java",
                "CheckIn.java",
                "ExceptionMapper.java",
                "TestDoerService.java",
                "Main.java").assertStatus(0);
        var result = java("demo.test.Main").assertStatus(0);

        System.out.println("--------Generated code (just for reference)-------");
        System.out.println(generatedService());

        assertEquals(expectedOut, result.stdOut());
    }

    @Test
    void maven__should_build_working_car_wash() throws Exception {
        mvn(workspace, "mvn-archetype", "archetype:generate", "-DgroupId=demo.test", "-DartifactId=my-app",
                "-DarchetypeArtifactId=maven-archetype-quickstart", "-DarchetypeVersion=1.4",
                "-DinteractiveMode=false").assertStatus(0);
        Path appFolder = workspace.resolve("my-app");
        Files.delete(appFolder.resolve("src/test/java/demo/test/AppTest.java"));
        Files.delete(appFolder.resolve("src/main/java/demo/test/App.java"));
        Path srcRoot = appFolder.resolve("src/main/java");
        writeCarWashClasses(srcRoot);
        writeSource(srcRoot, "TestDoerService.java", TEST_DOER_SERVICE);
        String expectedOut = writeCarWashMain(srcRoot);
        String dependencies = """
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
                  </dependencies>
                """.formatted(doerLibVersion, jakartaVersion);
        String plugins = """
                  <plugins>
                      <plugin>
                        <artifactId>maven-compiler-plugin</artifactId>
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
                    </plugins>
                  </build>
                """.formatted(doerLibVersion);
        String pom = Files.readString(appFolder.resolve("pom.xml"))
                .replaceAll("<maven.compiler.source>[^<]+</maven.compiler.source>",
                        "<maven.compiler.source>17</maven.compiler.source>")
                .replaceAll("<maven.compiler.target>[^<]+</maven.compiler.target>",
                        "<maven.compiler.target>17</maven.compiler.target>")
                .replace("</dependencies>", dependencies.strip())
                .replace("</build>", plugins.strip());
        Files.writeString(appFolder.resolve("pom.xml"), pom);

        mvn(appFolder, "mvn-package", "package").assertStatus(0);
        var result = mvn(appFolder, "mvn-exec", "-q", "exec:java", "-Dexec.mainClass=demo.test.Main")
                .assertStatus(0);

        assertEquals(expectedOut, result.stdOut());
    }

    /**
     * Classes for testing different allocation of doer methods and loaders and number of parameters.
     *
     * <p>Data objects passed to doer methods:
     * <ul>
     * <li>Soap - loaded but not saved (disposed after doer method call)
     * <li>Driver - loaded from demo.test2.Car and saved to CheckIn
     * <li>Car - loaded from CheckIn, saved in CarWash
     * <li>Washer - loaded from Bar, saved to Bar
     * </ul>
     *
     * <p>Doer methods: CarWash.washTheCar(task, car, washer, soap), Bar.takeACoffe(task, driver).
     *
     * <p>Loaders: CarWash.loadSoap(task), Bar.callFreeWasher(task), demo.test2.Car.inviteDriver(task),
     * CheckIn.takeACar(task).
     *
     * <p>Savers: CarWash.putCarToReadyQueue(task, car), Bar.releaseFreeWasher(task, washer),
     * CheckIn.checkoutDriver(task, driver).
     *
     * <p>Sources are flat in srcRoot, except demo.test2.Car: it is in test2/Car.java, because Car.java is
     * demo.test.Car.
     */
    static void writeCarWashClasses(Path srcRoot) throws Exception {
        writeSource(srcRoot, "Soap.java", """
                package demo.test;
                public class Soap {}
                """);
        writeSource(srcRoot, "Driver.java", """
                package demo.test;
                public class Driver {}
                """);
        writeSource(srcRoot, "Car.java", """
                package demo.test;
                public class Car {}
                """);
        writeSource(srcRoot, "test2/Car.java", """
                package demo.test2;
                import com.doer.*;
                import demo.test.Driver;
                public class Car {
                    @TaskDataLoader
                    public Driver inviteDriver(Task task) {
                        System.out.println(" Inviting driver");
                        return new Driver();
                    }
                }
                """);
        writeSource(srcRoot, "Washer.java", """
                package demo.test;
                public class Washer {}
                """);
        writeSource(srcRoot, "CarWash.java", """
                package demo.test;
                import com.doer.*;
                public class CarWash {
                    @AcceptStatus("Car is ready to be washed")
                    @AcceptStatus("Bus came to the wash")
                    public void washTheCar(Task task, Car car, Washer washer, Soap soap) {
                        System.out.println("  Washing the car (" + task.getStatus() + ")");
                        task.setStatus("Car wash complete");
                    }
                    @TaskDataLoader
                    public Soap loadSoap(Task task) {
                        System.out.println(" Loading soap");
                        return new Soap();
                    }
                    @TaskDataSaver
                    public void putCarToReadyQueue(Task task, Car car) {
                        System.out.println(" Putting car in ready queue");
                    }
                }
                """);
        writeSource(srcRoot, "Bar.java", """
                package demo.test;
                import com.doer.*;
                public class Bar {
                    @AcceptStatus("Driver wants a coffee")
                    public void takeACoffe(Task task, Driver driver) {
                        System.out.println("  Drinking coffee");
                        task.setStatus("Driver is happy with the coffee");
                    }
                    @TaskDataLoader
                    public Washer callFreeWasher(Task task) {
                        System.out.println(" Washer! Please help to new customer!");
                        return new Washer();
                    }
                    @TaskDataSaver
                    public void releaseFreeWasher(Task task, Washer washer) {
                        System.out.println(" Thank you, washer!");
                    }
                }
                """);
        writeSource(srcRoot, "CheckIn.java", """
                package demo.test;
                import com.doer.*;
                public class CheckIn {
                    @TaskDataLoader
                    public Car takeACar(Task task) {
                        System.out.println(" Taking customers car");
                        return new Car();
                    }
                    @TaskDataSaver
                    public void checkoutDriver(Task task, Driver driver) {
                        System.out.println(" Checkout.");
                    }
                }
                """);
        writeSource(srcRoot, "ExceptionMapper.java", """
                package demo.test;
                import com.doer.*;
                import jakarta.json.JsonObjectBuilder;
                public class ExceptionMapper {
                    @ExceptionDescriber
                    public void appendException1(Task task, Exception ex, JsonObjectBuilder builder) {
                        builder.add("appendException1", 1);
                    }
                    @ExceptionDescriber
                    public void appendRuntimeException2(Task task, RuntimeException ex, JsonObjectBuilder builder) {
                        builder.add("appendRuntimeException2", 2);
                    }
                }
                """);
    }

    /** Writes Main.java for the car wash classes; returns its expected output. */
    static String writeCarWashMain(Path srcRoot) throws Exception {
        writeSource(srcRoot, "Main.java", """
                package demo.test;
                import com.doer.*;
                public class Main {
                    public static void main(String[] args) throws Exception {
                        System.out.println("---main---");
                        var doer = new TestDoerService();
                        doer._inject_carWash(new CarWash());
                        doer._inject_bar(new Bar());
                        doer._inject_checkIn(new CheckIn());
                        doer._inject_car(new demo.test2.Car());

                        Task task = new Task();

                        System.out.println("Washing the car:");
                        task.setStatus("Car is ready to be washed");
                        doer.runTask(task);
                        System.out.println(task.getStatus());

                        System.out.println("Washing the Bus:");
                        task.setStatus("Bus came to the wash");
                        doer.runTask(task);
                        System.out.println(task.getStatus());

                        System.out.println("Taking a coffee:");
                        task.setStatus("Driver wants a coffee");
                        doer.runTask(task);
                        System.out.println(task.getStatus());

                        System.out.println("---end-of-main---");
                    }
                }
                """);
        return """
                ---main---
                Washing the car:
                 Taking customers car
                 Washer! Please help to new customer!
                 Loading soap
                  Washing the car (Car is ready to be washed)
                 Thank you, washer!
                 Putting car in ready queue
                Car wash complete
                Washing the Bus:
                 Taking customers car
                 Washer! Please help to new customer!
                 Loading soap
                  Washing the car (Bus came to the wash)
                 Thank you, washer!
                 Putting car in ready queue
                Car wash complete
                Taking a coffee:
                 Inviting driver
                  Drinking coffee
                 Checkout.
                Driver is happy with the coffee
                ---end-of-main---
                """;
    }
}
