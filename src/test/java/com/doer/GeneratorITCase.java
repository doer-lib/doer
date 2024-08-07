package com.doer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.io.TempDir;

import static com.doer.Utils.*;
import static io.restassured.path.json.JsonPath.with;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GeneratorITCase {
    @TempDir
    static Path dir;
    static String jakartaVersion;
    static String jakartaPackage;
    static String doerLibVersion = System.getProperty("doer.lib.version", "1.0-SNAPSHOT");

    String doerJackarta;
    String doerJackartaClasses;

    @BeforeAll
    static void initTestFolder() throws Exception {
        Path projectDir = new File(".").toPath();
        Path jarPath = Paths.get("target", "doer-" + doerLibVersion + ".jar");
        Path localRepo = dir.resolve("m2-local");
        // Create local maven repository to use with test projects
        System.out.println("Creating local maven repository for tests in " + localRepo);
        long t0 = System.currentTimeMillis();

        Files.createDirectory(localRepo);

        jakartaVersion = System.getProperty("test.doer.jakarta.version", "10.0.0");
        jakartaPackage = "8.0.0".equals(jakartaVersion) ? "javax" : "jakarta";
        exec(0, dir, "mvn -Dmaven.repo.local=m2-local dependency:get -Dartifact=jakarta.platform:jakarta.jakartaee-api:"
                        + jakartaVersion);
        exec(0, projectDir,
                "mvn install:install-file ? -DgroupId=com.doer -DartifactId=doer ? -Dpackaging=jar -DgeneratePom=true ?",
                "-Dfile=" + jarPath,
                "-Dversion=" + doerLibVersion,
                "-DlocalRepositoryPath=" + localRepo);
        System.out.printf("✔ Created in %s seconds%n", (System.currentTimeMillis() - t0) / 1000);
    }

    @BeforeEach
    void clearTestFolder() throws IOException {
        Path m2local = dir.resolve("m2-local");
        Files.list(dir)
                .filter(f -> !m2local.equals(f))
                .forEach(Utils::deleteReqursivelly);
        Files.createDirectories(dir.resolve("classes"));

        String sep = System.getProperty("path.separator");
        doerJackarta = "m2-local/jakarta/platform/jakarta.jakartaee-api/" + jakartaVersion +
                "/jakarta.jakartaee-api-" + jakartaVersion + ".jar" + sep +
                "m2-local/com/doer/doer/"+ doerLibVersion + "/doer-" + doerLibVersion + ".jar";
        doerJackartaClasses = doerJackarta + sep + "classes";
    }

    @Test
    void javac__should_handle_AcceptStatus() throws Exception {
        String code1 = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "public class Doer1 {\n" +
                "    @AcceptStatus(\"A\")\n" +
                "    public void method1(Task task) {\n" +
                "        System.out.println(\"Doer1 method1 called\");\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Doer1.java"), code1.getBytes(UTF_8));
        String code2 = "package demo.test;\n" +
                "import com.doer.*;\n" +
                "import java.sql.*;\n" +
                "import com.doer.generated._GeneratedDoerService;\n" +
                "public class Main {\n" +
                "    public static void main(String[] args) throws Exception {\n" +
                "        System.out.println(\"Main method called\");\n" +
                "        _GeneratedDoerService doer = new _GeneratedDoerService() {\n" +
                "            public boolean updateAndBumpVersion(Task task) throws SQLException {\n" +
                "                return true;\n" +
                "            }\n" +
                "            public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className, String methodName,\n"
                +
                "            String exceptionType, String extraJson, Integer durationMs) throws SQLException {\n"
                +
                "                return 1;\n" +
                "            }\n" +
                "        };\n" +
                "        doer._inject_SelfReference(doer);\n" +
                "        doer._inject_doer1(new Doer1());\n" +
                "        Task task = new Task();\n" +
                "        task.setStatus(\"A\");\n" +
                "        doer.runTask(task);\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Main.java"), code2.getBytes(UTF_8));

        // DEBUGGING HELP: Add this args to javac to attach debugger to annotation
        // processor.
        // "-J-Xdebug -J-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000",
        // -J-ea - enable accertion in annotation processor
        exec(0, dir, "javac -J-ea -cp ? -d classes Doer1.java Main.java", doerJackarta);

        String result = exec(0, dir, "java -cp ? demo.test.Main", doerJackartaClasses);

        assertEquals("Main method called\nDoer1 method1 called\n", result);
    }

    @Test
    void javac__should_handle_multiple_AcceptStatus() throws Exception {
        String code1 = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "public class Doer1 {\n" +
                "    @AcceptStatus(\"A\")\n" +
                "    @AcceptStatus(\"B\")\n" +
                "    public void method2(Task task) {\n" +
                "        System.out.println(\"---\" + task.getStatus() + \"---\");\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Doer1.java"), code1.getBytes(UTF_8));
        String code2 = "package demo.test;\n" +
                "import com.doer.*;\n" +
                "import java.sql.*;\n" +
                "import com.doer.generated._GeneratedDoerService;\n" +
                "public class Main {\n" +
                "    public static void main(String[] args) throws Exception {\n" +
                "        System.out.println(\"---main---\");\n" +
                "        _GeneratedDoerService doer = new _GeneratedDoerService() {\n" +
                "            public boolean updateAndBumpVersion(Task task) throws SQLException {\n" +
                "                return true;\n" +
                "            }\n" +
                "            public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className, String methodName,\n"
                +
                "            String exceptionType, String extraJson, Integer durationMs) throws SQLException {\n"
                +
                "                return 1;\n" +
                "            }\n" +
                "        };\n" +
                "        doer._inject_SelfReference(doer);\n" +
                "        doer._inject_doer1(new Doer1());\n" +
                "        Task task = new Task();\n" +
                "        task.setStatus(\"A\");\n" +
                "        doer.runTask(task);\n" +
                "        task.setStatus(\"B\");\n" +
                "        doer.runTask(task);\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Main.java"), code2.getBytes(UTF_8));
        exec(0, dir, "javac -J-ea -cp ? -d classes Doer1.java Main.java", doerJackarta);

        String result = exec(0, dir, "java -cp ? demo.test.Main", doerJackartaClasses);

        assertEquals("---main---\n---A---\n---B---\n", result);
    }

    @Test
    void javac__should_use_loaders() throws Exception {
        String code1 = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "public class Doer1 {\n" +
                "    @AcceptStatus(\"A\")\n" +
                "    public void method2(DemoClass data, Task task) {\n" +
                "        System.out.println(\"---\" + task.getStatus() + \"---\" + data.name + \"---\");\n"
                +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Doer1.java"), code1.getBytes(UTF_8));
        String code2 = "" +
                "package demo.test;\n" +
                "public class DemoClass {\n" +
                "    public String name;\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("DemoClass.java"), code2.getBytes(UTF_8));
        String code3 = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "public class DemoClassLoader {\n" +
                "    @DoerLoader\n" +
                "    public DemoClass loadDemoClass(Task task) {\n" +
                "        System.out.println(\"---load-DemoClass-for-\" + task.getStatus() + \"---\");\n"
                +
                "        DemoClass c = new DemoClass();\n" +
                "        c.name = \"d1\";\n" +
                "        return c;\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("DemoClassLoader.java"), code3.getBytes(UTF_8));
        String code4 = "package demo.test;\n" +
                "import com.doer.*;\n" +
                "import java.sql.*;\n" +
                "import com.doer.generated._GeneratedDoerService;\n" +
                "public class Main {\n" +
                "    public static void main(String[] args) throws Exception {\n" +
                "        System.out.println(\"---main---\");\n" +
                "        _GeneratedDoerService doer = new _GeneratedDoerService() {\n" +
                "            public boolean updateAndBumpVersion(Task task) throws SQLException {\n" +
                "                return true;\n" +
                "            }\n" +
                "            public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className, String methodName,\n"
                +
                "            String exceptionType, String extraJson, Integer durationMs) throws SQLException {\n"
                +
                "                return 1;\n" +
                "            }\n" +
                "        };\n" +
                "        doer._inject_SelfReference(doer);\n" +
                "        doer._inject_doer1(new Doer1());\n" +
                "        doer._inject_demoClassLoader(new DemoClassLoader());\n" +
                "        Task task = new Task();\n" +
                "        task.setStatus(\"A\");\n" +
                "        doer.runTask(task);\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Main.java"), code4.getBytes(UTF_8));
        exec(0, dir, "javac -J-ea -cp ? -d classes Doer1.java DemoClass.java DemoClassLoader.java Main.java",
                doerJackarta);

        String result = exec(0, dir, "java -cp ? demo.test.Main", doerJackartaClasses);

        assertEquals("---main---\n---load-DemoClass-for-A---\n---A---d1---\n", result);
    }

    @Test
    void javac__should_use_unloader() throws Exception {
        String code = "package demo.test;\n" +
                "import com.doer.*;\n" +
                "import java.sql.*;\n" +
                "import com.doer.generated._GeneratedDoerService;\n" +
                "public class Main {\n" +
                "    public static void main(String[] args) throws Exception {\n" +
                "        System.out.println(\"---main---\");\n" +
                "        _GeneratedDoerService doer = new _GeneratedDoerService() {\n" +
                "            public boolean updateAndBumpVersion(Task task) throws SQLException {\n" +
                "                return true;\n" +
                "            }\n" +
                "            public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className, String methodName,\n"
                +
                "            String exceptionType, String extraJson, Integer durationMs) throws SQLException {\n"
                +
                "                return 1;\n" +
                "            }\n" +
                "        };\n" +
                "        doer._inject_SelfReference(doer);\n" +
                "        doer._inject_main(new Main());\n" +

                "        Task task = new Task();\n" +
                "        task.setStatus(\"A\");\n" +
                "        doer.runTask(task);\n" +
                "    }\n" +
                "    @AcceptStatus(\"A\")\n" +
                "    public void doerMethod(Task task, String data) {\n" +
                "        System.out.println(\"---\" + task.getStatus() + \"---\" + data + \"---\");\n" +
                "    }\n" +
                "    @DoerLoader\n" +
                "    public String loaderMethod(Task task) {\n" +
                "        System.out.println(\"---load-String-for-\" + task.getStatus() + \"---\");\n" +
                "        return \"d2\";\n" +
                "    }\n" +
                "    @DoerUnloader\n" +
                "    public void unloderMethod(Task task, String data) {\n" +
                "        System.out.println(\"---unload-String-for-\" + task.getStatus() + \"---\" + data + \"---\");\n"
                +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Main.java"), code.getBytes(UTF_8));
        exec(0, dir, "javac -J-ea -cp ? -d classes Main.java", doerJackarta);

        String result = exec(0, dir, "java -cp ? demo.test.Main", doerJackartaClasses);

        assertEquals("---main---\n---load-String-for-A---\n---A---d2---\n---unload-String-for-A---d2---\n",
                result);
    }

    @Test
    void javac__should_do_all_variants() throws Exception {
        generateCarWashDemoClasses(dir);
        String expectedResult = generateMainClassForCarWathAndReturnExpectedResult(dir);
        List<String> files = Arrays.asList(
                "demo/test/Soap.java",
                "demo/test/Driver.java",
                "demo/test/Car.java",
                "demo/test2/Car.java",
                "demo/test/Washer.java",
                "demo/test/CarWash.java",
                "demo/test/Bar.java",
                "demo/test/CheckIn.java",
                "demo/test/ExceptionMapper.java",
                "demo/test/Main.java");
        exec(0, dir, "javac -J-ea -cp ? -d classes " + String.join(" ", files), doerJackarta);

        String result = exec(0, dir, "java -cp ? demo.test.Main", doerJackartaClasses);

        System.out.println("--------Generated code (just for reference)-------");
        Files.copy(dir.resolve("classes/com/doer/generated/_GeneratedDoerService.java"), System.out);

        assertTrue(expectedResult.contains("Washing the car (Bus came to the wash)"),
                "Just checking expetedResult is not messed up");
        assertEquals(expectedResult, result);
    }

    @Test
    void mvn__should_do_all_variants() throws Exception {
        exec(0, dir,
                "mvn -Dmaven.repo.local=m2-local archetype:generate -DgroupId=demo.test -DartifactId=my-app -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.4 -DinteractiveMode=false");
        Path appFolder = dir.resolve("my-app");
        Files.delete(appFolder.resolve("src/test/java/demo/test/AppTest.java"));
        Files.delete(appFolder.resolve("src/main/java/demo/test/App.java"));
        Path srcRoot = Files.createDirectories(appFolder.resolve("src/main/java"));
        generateCarWashDemoClasses(srcRoot);
        String expectedResult = generateMainClassForCarWathAndReturnExpectedResult(srcRoot);
        String additionalDependencies = "" +
                "  <dependency>\n" +
                "      <groupId>com.doer</groupId>\n" +
                "      <artifactId>doer</artifactId>\n" +
                "      <version>" + doerLibVersion + "</version>\n" +
                "    </dependency>\n" +
                "    <dependency>\n" +
                "      <groupId>jakarta.platform</groupId>\n" +
                "      <artifactId>jakarta.jakartaee-api</artifactId>\n" +
                "      <version>" + jakartaVersion + "</version>\n" +
                "    </dependency>\n" +
                "";
        String pom = new String(Files.readAllBytes(appFolder.resolve("pom.xml")))
                .replaceAll("<maven.compiler.source>[^<]+</maven.compiler.source>",
                        "<maven.compiler.source>1.8</maven.compiler.source>")
                .replaceAll("<maven.compiler.target>[^<]+</maven.compiler.target>",
                        "<maven.compiler.target>1.8</maven.compiler.target>")
                .replaceAll("</dependencies>", additionalDependencies + "  </dependencies>");
        Files.write(appFolder.resolve("pom.xml"), pom.getBytes(UTF_8));

        exec(0, appFolder, "mvn -Dmaven.repo.local=../m2-local package");
        String result = exec(0, appFolder,
                "mvn -q -Dmaven.repo.local=../m2-local exec:java -Dexec.mainClass=demo.test.Main");
        assertTrue(expectedResult.contains("Washing the car (Bus came to the wash)"),
                "Just checking expectedResult is not messed up");
        assertEquals(expectedResult, result);

    }

    void generateCarWashDemoClasses(Path srcRoot) throws IOException {
        // Classes set for testing different allocation of doer methods and loaders and
        // number of
        // parameters

        // Data objects passed to doer methods:
        // Soap - loaded but not unloaded (disposed after doer method call)
        // Driver - loaded from demo.test2.Car and unloaded to Checkin
        // Car - loaded from CheckIn, unloaded in CarWash
        // Washer - loaded from Bar, unloaded to bar

        // Doer methods:
        // CarWash.washTheCar(task, car, washer, soap)
        // Bar.takeACoffe(task, driver)

        // Loaders/unloaders:
        // Soap CarWash.loadSoap(task)
        // Washer Bar.callFreeWasher(task)
        // Driver demo.test2.Car.inviteDriver(task)
        // Car CheckIn.takeACar(task)
        // CarWash.putCarToReadyQueue(task, car)
        // Bar.releaseFreeWasher(task, washer)
        // CheckIn.checkoutDriver(task, Driver)

        Path dir = Files.createDirectories(srcRoot.resolve("demo/test"));
        Path dir2 = Files.createDirectories(srcRoot.resolve("demo/test2"));
        String soapCode = "" +
                "package demo.test;\n" +
                "public class Soap {}\n" +
                "";
        Files.write(dir.resolve("Soap.java"), soapCode.getBytes(UTF_8));
        String driverCode = "" +
                "package demo.test;\n" +
                "public class Driver {}\n" +
                "";
        Files.write(dir.resolve("Driver.java"), driverCode.getBytes(UTF_8));
        String carCode = "" +
                "package demo.test;\n" +
                "public class Car {}\n" +
                "";
        Files.write(dir.resolve("Car.java"), carCode.getBytes(UTF_8));
        String car2Code = "" +
                "package demo.test2;\n" +
                "import com.doer.*;\n" +
                "import demo.test.Driver;\n" +
                "public class Car {\n" +
                "    @DoerLoader\n" +
                "    public Driver inviteDriver(Task task) {\n" +
                "        System.out.println(\" Inviting driver\");\n" +
                "        return new Driver();\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir2.resolve("Car.java"), car2Code.getBytes(UTF_8));
        String washerCode = "" +
                "package demo.test;\n" +
                "public class Washer {}\n" +
                "";
        Files.write(dir.resolve("Washer.java"), washerCode.getBytes(UTF_8));
        String carWashCode = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "public class CarWash {\n" +
                "    @AcceptStatus(\"Car is ready to be washed\")\n" +
                "    @AcceptStatus(\"Bus came to the wash\")\n" +
                "    public void washTheCar(Task task, Car car, Washer washer, Soap soap) {\n" +
                "        System.out.println(\"  Washing the car (\" + task.getStatus() + \")\");\n" +
                "        task.setStatus(\"Car wash complete\");\n" +
                "    }\n" +
                "    @DoerLoader\n" +
                "    public Soap loadSoap(Task task) {\n" +
                "        System.out.println(\" Loading soap\");\n" +
                "        return new Soap();\n" +
                "    }\n" +
                "    @DoerUnloader\n" +
                "    public void putCarToReadyQueue(Task task, Car car) {\n" +
                "        System.out.println(\" Putting car in ready queue\");\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("CarWash.java"), carWashCode.getBytes(UTF_8));
        String barCode = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "public class Bar {\n" +
                "    @AcceptStatus(\"Driver wants a coffee\")\n" +
                "    public void takeACoffe(Task task, Driver driver) {\n" +
                "        System.out.println(\"  Drinking coffee\");\n" +
                "        task.setStatus(\"Driver is happy with the coffee\");\n" +
                "    }\n" +
                "    @DoerLoader\n" +
                "    public Washer callFreeWasher(Task task) {\n" +
                "        System.out.println(\" Washer! Please help to new customer!\");\n" +
                "        return new Washer();\n" +
                "    }\n" +
                "    @DoerUnloader\n" +
                "    public void releaseFreeWasher(Task task, Washer washer) {\n" +
                "        System.out.println(\" Thank you, washer!\");\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Bar.java"), barCode.getBytes(UTF_8));
        String checkInCode = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "public class CheckIn {\n" +
                "    @DoerLoader\n" +
                "    public Car takeACar(Task task) {\n" +
                "        System.out.println(\" Taking customers car\");\n" +
                "        return new Car();\n" +
                "    }\n" +
                "    @DoerUnloader\n" +
                "    public void checkoutDriver(Task task, Driver driver) {\n" +
                "        System.out.println(\" Checkout.\");\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("CheckIn.java"), checkInCode.getBytes(UTF_8));
        String exMapperCode = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "import " + jakartaPackage + ".json.JsonObjectBuilder;\n" +
                "public class ExceptionMapper {\n" +
                "    @DoerExtraJson\n" +
                "    public void appendException1(Task task, Exception ex, JsonObjectBuilder builder) {\n" +
                "        builder.add(\"appendException1\", 1);\n" +
                "    }\n" +
                "    @DoerExtraJson\n" +
                "    public void appendRuntimeException2(Task task, RuntimeException ex, JsonObjectBuilder builder) {\n" +
                "        builder.add(\"appendRuntimeException2\", 2);\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("ExceptionMapper.java"), exMapperCode.getBytes(UTF_8));
    }

    String generateMainClassForCarWathAndReturnExpectedResult(Path srcRoot) throws IOException {
        Path dir = Files.createDirectories(srcRoot.resolve("demo/test"));
        String mainCode = "" +
                "package demo.test;\n" +
                "import com.doer.*;\n" +
                "import java.sql.*;\n" +
                "import com.doer.generated._GeneratedDoerService;\n" +
                "public class Main {\n" +
                "    public static void main(String[] args) throws Exception {\n" +
                "        System.out.println(\"---main---\");\n" +
                "        _GeneratedDoerService doer = new _GeneratedDoerService() {\n" +
                "            public boolean updateAndBumpVersion(Task task) throws SQLException {\n" +
                "                return true;\n" +
                "            }\n" +
                "            public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className, String methodName,\n"
                +
                "            String exceptionType, String extraJson, Integer durationMs) throws SQLException {\n"
                +
                "                return 1;\n" +
                "            }\n" +
                "        };\n" +
                "        doer._inject_SelfReference(doer);\n" +
                "        doer._inject_carWash(new CarWash());\n" +
                "        doer._inject_bar(new Bar());\n" +
                "        doer._inject_checkIn(new CheckIn());\n" +
                "        doer._inject_car(new demo.test2.Car());\n" +
                "\n" +
                "        Task task = new Task();\n" +
                "\n" +
                "        System.out.println(\"Washing the car:\");\n" +
                "        task.setStatus(\"Car is ready to be washed\");\n" +
                "        doer.runTask(task);\n" +
                "        System.out.println(task.getStatus());\n" +
                "\n" +
                "        System.out.println(\"Washing the Bus:\");\n" +
                "        task.setStatus(\"Bus came to the wash\");\n" +
                "        doer.runTask(task);\n" +
                "        System.out.println(task.getStatus());\n" +
                "\n" +
                "        System.out.println(\"Taking a coffee:\");\n" +
                "        task.setStatus(\"Driver wants a coffee\");\n" +
                "        doer.runTask(task);\n" +
                "        System.out.println(task.getStatus());\n" +
                "\n" +
                "        System.out.println(\"---end-of-main---\");\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Main.java"), mainCode.getBytes(UTF_8));
        String expectedResult = "" +
                "---main---\n" +
                "Washing the car:\n" +
                " Taking customers car\n" +
                " Washer! Please help to new customer!\n" +
                " Loading soap\n" +
                "  Washing the car (Car is ready to be washed)\n" +
                " Thank you, washer!\n" +
                " Putting car in ready queue\n" +
                "Car wash complete\n" +
                "Washing the Bus:\n" +
                " Taking customers car\n" +
                " Washer! Please help to new customer!\n" +
                " Loading soap\n" +
                "  Washing the car (Bus came to the wash)\n" +
                " Thank you, washer!\n" +
                " Putting car in ready queue\n" +
                "Car wash complete\n" +
                "Taking a coffee:\n" +
                " Inviting driver\n" +
                "  Drinking coffee\n" +
                " Checkout.\n" +
                "Driver is happy with the coffee\n" +
                "---end-of-main---\n" +
                "";
        return expectedResult;
    }

    @Test
    void javac__should_find_setStatus_on_parameter() throws Exception {
        String code = "" +
                "package demo.test;\n" +
                "import java.util.Collections;\n" +
                "import com.doer.*;\n" +
                "public class Flamingo {\n" +
                "    boolean x = false;\n" +
                "    @AcceptStatus(\"A1\")\n" +
                "    public void fly(Task task) {\n" +
                "        task.setStatus(\"In Fly 1\");\n" +
                "        if (task.isInProgress()) {\n" +
                "            task.setStatus(\"In Fly 2\");\n" +
                "        } else {\n" +
                "            task.setStatus(\"In Fly 3\");\n" +
                "        }\n" +
                "        ((Runnable)() -> task.setStatus(\"In Fly 4\")).run();\n" +
                "        task.setStatus(task.isInProgress() ? \"In Fly 5\" : (x ? \"In Fly 6\" : \"In Fly 7\"));\n"
                +
                "        switch (task.getStatus()) {\n" +
                "            case \"A\":\n" +
                "                task.setStatus(\"In Fly A\");\n" +
                "                break;\n" +
                "            default:\n" +
                "                task.setStatus(\"In Fly Default\");\n" +
                "                break;\n" +
                "        }\n" +
                "        if(x) {} else {}\n" +
                "        if(x) {}\n" +
                "        try {\n" +
                "            task.setStatus(\"In Fly 10\");\n" +
                "        } catch (RuntimeException e) {\n" +
                "            task.setStatus(\"In Fly 11\");\n" +
                "        } finally {\n" +
                "            task.setStatus(\"In Fly 12\");\n" +
                "        }\n" +
                "        for(String a : Collections.singleton(\"a\")) {\n" +
                "            task.setStatus(\"In Fly 13\");\n" +
                "        }\n" +
                "        for(int i = 0; i < 1; i++) {\n" +
                "            task.setStatus(\"In Fly 14\");\n" +
                "        }\n" +
                "        while(x) {\n" +
                "            task.setStatus(\"In Fly 15\");\n" +
                "        }\n" +
                "        do {\n" +
                "            task.setStatus(\"In Fly 16\");\n" +
                "        } while(x);\n" +
                "        synchronized(this) {\n" +
                "            task.setStatus(\"In Fly 17\");\n" +
                "        }\n" +
                "        switch(task.getStatus()) {}\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Flamingo.java"), code.getBytes(UTF_8));

        exec(0, dir, "javac -J-ea -cp ? -d classes Flamingo.java",
                doerJackarta);
        
        File doerJson = new File(dir.toFile(), "classes/com/doer/generated/doer.json");
        List<String> ressult = with(doerJson)
                .get("doer_methods[0].emits");
        List<String> expected = Arrays.asList("In Fly 1",
                "In Fly 10",
                "In Fly 11",
                "In Fly 12",
                "In Fly 13",
                "In Fly 14",
                "In Fly 15",
                "In Fly 16",
                "In Fly 17",
                "In Fly 2",
                "In Fly 3",
                "In Fly 4",
                "In Fly 5",
                "In Fly 6",
                "In Fly 7",
                "In Fly A",
                "In Fly Default");
        assertEquals(expected, ressult);
    }

    @DisabledOnJre(JRE.JAVA_8)
    @Test
    void javac__should_detect_statuses_set_by_constants() throws Exception {
        String code1 = "" +
                "package demo.test;\n" +
                "public interface Statuses1 {\n" +
                "    String ST1_1 = \"ST1_1_value\";\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Statuses1.java"), code1.getBytes(UTF_8));
        String code2 = "" +
                "package demo.test;\n" +
                "public abstract class Statuses2 {\n" +
                "    final public static String ST2_2 = \"ST2_2_value\";\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Statuses2.java"), code2.getBytes(UTF_8));
        String code3 = "" +
                "package demo.test;\n" +
                "import java.util.Collections;\n" +
                "import com.doer.*;\n" +
                "import static demo.test.Statuses2.*;\n" +
                "public class Flamingo implements Statuses1 {\n" +
                "    @AcceptStatus(\"Z1\")\n" +
                "    public void fly(Task task) {\n" +
                "        task.setStatus(ST1_1);\n" +
                "        task.setStatus(ST2_2);\n" +
                "    }\n" +
                "}\n" +
                "";
        Files.write(dir.resolve("Flamingo.java"), code3.getBytes(UTF_8));

        exec(0, dir, "javac -J-ea -cp ? -d classes Flamingo.java Statuses1.java Statuses2.java",
                doerJackarta);

        File doerJson = new File(dir.toFile(), "classes/com/doer/generated/doer.json");
        List<String> list = with(doerJson)
                .get("doer_methods[0].emits");
        assertEquals(Arrays.asList("ST1_1_value", "ST2_2_value"), list);
    }

}
