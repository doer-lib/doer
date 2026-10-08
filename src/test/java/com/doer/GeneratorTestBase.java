package com.doer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import io.restassured.path.json.JsonPath;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Base for tests that compile code with DoerProcessor. Every test works in its own workspace folder, which is
 * cleared before the test and kept after it, so after a failure it has the sources, the compiled and generated files
 * and the output of every command:
 *
 * <pre>
 * target/it-test-workspaces/&lt;test class&gt;/_beforeAll/    the same for {@code @BeforeAll}, and mvn-*.txt
 * target/it-test-workspaces/&lt;test class&gt;/&lt;test method&gt;/
 *     Doer1.java, Main.java, ...   sources, flat (javac does not require folders to match packages)
 *     classes/demo/test/*.class    compiled classes, by package
 *     classes/com/doer/generated/  _GeneratedDoerService.java, doer.json, doer.dot
 *     javac-*.txt, java-*.txt      command, stdout and stderr of each run
 * </pre>
 */
public abstract class GeneratorTestBase {
    static final Path WORKSPACES = Path.of("target", "it-test-workspaces").toAbsolutePath();
    static final int TIMEOUT_SECONDS = 60;
    static final int MVN_TIMEOUT_SECONDS = 300;
    static final String SEP = System.getProperty("path.separator");

    static String jakartaVersion = System.getProperty("test.doer.jakarta.version", "10.0.0");
    static String parssonVersion = System.getProperty("test.doer.parsson.version", "1.1.7");
    static String doerLibVersion = System.getProperty("doer.lib.version", "0.0.0-IT-SNAPSHOT");
    static Path m2Repo = Path.of(System.getProperty("doer.test.m2.repo",
            System.getProperty("user.home") + "/.m2/repository"));

    /** Jakarta EE API and doer jars. */
    static String compileClasspath;
    /** Compile classpath plus Jakarta JSON implementation and the compiled classes. */
    static String runClasspath;

    /** Workspace folder of {@code @BeforeAll} of the test class, created before subclasses' {@code @BeforeAll}. */
    static Path beforeAllWorkspace;

    /** Workspace folder of the current test. */
    Path workspace;

    @BeforeAll
    static void resolveDependencies(TestInfo info) throws Exception {
        beforeAllWorkspace = WORKSPACES.resolve(info.getTestClass().orElseThrow().getSimpleName())
                .resolve("_beforeAll");
        Utils.deleteRecursively(beforeAllWorkspace);
        Files.createDirectories(beforeAllWorkspace);
        RunResult jakarta = mvn(beforeAllWorkspace, "mvn-dependency-get-jakarta",
                "dependency:get", "-Dartifact=jakarta.platform:jakarta.jakartaee-api:" + jakartaVersion)
                .assertStatus(0);
        RunResult parsson = mvn(beforeAllWorkspace, "mvn-dependency-get-parsson",
                "dependency:get", "-Dartifact=org.eclipse.parsson:parsson:" + parssonVersion)
                .assertStatus(0);
        System.out.printf("✔ Dependencies resolved in %s ms%n", jakarta.runMilliseconds() + parsson.runMilliseconds());

        compileClasspath = jar("jakarta/platform/jakarta.jakartaee-api", jakartaVersion, "jakarta.jakartaee-api")
                + SEP + jar("com/java-doer/doer", doerLibVersion, "doer");
        runClasspath = compileClasspath + SEP + jar("org/eclipse/parsson/parsson", parssonVersion, "parsson")
                + SEP + "classes";
    }

    @BeforeEach
    void prepareWorkspace(TestInfo info) throws IOException {
        workspace = WORKSPACES.resolve(getClass().getSimpleName()).resolve(info.getTestMethod().orElseThrow().getName());
        Utils.deleteRecursively(workspace);
        Files.createDirectories(workspace.resolve("classes"));
    }

    /** Writes the source file (path is relative to the test workspace). */
    void writeSource(String path, String code) throws IOException {
        writeSource(workspace, path, code);
    }

    public static void writeSource(Path root, String path, String code) throws IOException {
        Path file = root.resolve(path);
        Files.createDirectories(file.getParent());
        Files.writeString(file, code);
    }

    /**
     * Source of TestDoerService.java: the generated service that works without database. It must be compiled
     * together with the doer classes. It makes public _load and _save, which DoerService.facilitateCoordinatedUpdate
     * uses to load and save task data by type.
     */
    static final String TEST_DOER_SERVICE = """
            package demo.test;
            import com.doer.*;
            import com.doer.generated._GeneratedDoerService;
            public class TestDoerService extends _GeneratedDoerService {
                public TestDoerService() {
                    setSelfReference(this);
                }
                @Override
                public boolean updateAndBumpVersion(Task task) {
                    return true;
                }
                @Override
                public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className,
                        String methodName, String exceptionType, String extraJson, Integer durationMs) {
                    return 1;
                }
                @Override
                public Object _load(Task task, Class<?> type) throws Exception {
                    return super._load(task, type);
                }
                @Override
                public void _save(Task task, Class<?> type, Object data) throws Exception {
                    super._save(task, type, data);
                }
            }
            """;

    /**
     * Compiles the files with DoerProcessor into the classes folder. The output is kept in javac-out.txt and
     * javac-err.txt (javac writes its messages to stderr).
     */
    RunResult javac(String... optionsAndFiles) throws Exception {
        return javac(workspace, optionsAndFiles);
    }

    /** Same as {@link #javac(String...)}, in the given folder (for {@code @BeforeAll}). */
    static RunResult javac(Path workDir, String... optionsAndFiles) throws Exception {
        // DEBUGGING HELP: add "-J-Xdebug -J-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000"
        // to attach debugger to the annotation processor. -J-ea enables assertions in the annotation processor.
        List<String> cmd = new ArrayList<>(List.of("javac", "-J-ea", "-processor",
                "com.doer.processor.DoerProcessor", "-cp", compileClasspath, "-d", "classes"));
        cmd.addAll(List.of(optionsAndFiles));
        return Utils.run(workDir, "javac", TIMEOUT_SECONDS, cmd);
    }

    /** Runs the compiled class. The output is kept in java-out.txt and java-err.txt. */
    RunResult java(String mainClass) throws Exception {
        return Utils.run(workspace, "java", TIMEOUT_SECONDS, List.of("java", "-cp", runClasspath, mainClass));
    }

    /** Runs mvn in batch mode. The output is kept in &lt;logName&gt;-out.txt and &lt;logName&gt;-err.txt. */
    static RunResult mvn(Path workDir, String logName, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("mvn", "-B"));
        cmd.addAll(List.of(args));
        return Utils.run(workDir, logName, MVN_TIMEOUT_SECONDS, cmd);
    }

    /** File generated by DoerProcessor. */
    Path generated(String name) {
        return workspace.resolve("classes/com/doer/generated").resolve(name);
    }

    String generatedService() throws IOException {
        return Files.readString(generated("_GeneratedDoerService.java"));
    }

    JsonPath doerJson() {
        return JsonPath.from(generated("doer.json").toFile());
    }

    private static String jar(String groupPath, String version, String artifactId) {
        return m2Repo.resolve(groupPath).resolve(version).resolve(artifactId + "-" + version + ".jar").toString();
    }
}
