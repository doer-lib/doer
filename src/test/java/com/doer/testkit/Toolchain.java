package com.doer.testkit;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * javac, java and mvn, run in a workspace folder, and the jars they need from the local Maven repository. The output
 * of every command is kept in the folder (see {@link Processes#run}).
 * <p>
 * Properties: {@code doer.lib.version} (the doer jar under test, {@code 0.0.0-IT-SNAPSHOT}),
 * {@code doer.test.m2.repo}, {@code test.doer.jakarta.version}, {@code test.doer.parsson.version}.
 */
public final class Toolchain {
    public static final int TIMEOUT_SECONDS = 60;
    public static final int MVN_TIMEOUT_SECONDS = 600;

    public static final String jakartaVersion = property("test.doer.jakarta.version", "10.0.0");
    public static final String parssonVersion = property("test.doer.parsson.version", "1.1.7");
    public static final String doerLibVersion = property("doer.lib.version", "0.0.0-IT-SNAPSHOT");
    public static final Path m2Repo = Path.of(property("doer.test.m2.repo",
            System.getProperty("user.home") + "/.m2/repository"));

    private static String compileClasspath;

    private Toolchain() {
    }

    /**
     * Jakarta EE API and doer jars. On the first call in the JVM the Jakarta EE API and Parsson are downloaded into
     * the local repository; the output is kept in {@code target/it-test-workspaces/_dependencies}.
     */
    public static synchronized String compileClasspath() throws Exception {
        if (compileClasspath == null) {
            Path workDir = Workspaces.recreate(Workspaces.ROOT.resolve("_dependencies"));
            RunResult jakarta = mvn(workDir, "mvn-dependency-get-jakarta",
                    "dependency:get", "-Dartifact=jakarta.platform:jakarta.jakartaee-api:" + jakartaVersion)
                    .assertStatus(0);
            RunResult parsson = mvn(workDir, "mvn-dependency-get-parsson",
                    "dependency:get", "-Dartifact=org.eclipse.parsson:parsson:" + parssonVersion)
                    .assertStatus(0);
            System.out.printf("✔ Dependencies resolved in %s ms%n",
                    jakarta.runMilliseconds() + parsson.runMilliseconds());
            compileClasspath = classpath(
                    jar("jakarta/platform/jakarta.jakartaee-api", jakartaVersion, "jakarta.jakartaee-api"),
                    jar("com/java-doer/doer", doerLibVersion, "doer"));
        }
        return compileClasspath;
    }

    /** Compile classpath plus Jakarta JSON implementation and the folder of compiled classes. */
    public static String runClasspath(String classesFolder) throws Exception {
        return classpath(compileClasspath(), jar("org/eclipse/parsson/parsson", parssonVersion, "parsson"),
                classesFolder);
    }

    /**
     * Compiles the files with DoerProcessor into the output folder. The output is kept in
     * {@code <logName>-out.txt} and {@code <logName>-err.txt} (javac writes its messages to stderr).
     */
    public static RunResult javac(Path workDir, String logName, String classpath, String outputFolder,
            String... optionsAndFiles) throws Exception {
        Files.createDirectories(workDir.resolve(outputFolder));
        // DEBUGGING HELP: add "-J-Xdebug -J-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000"
        // to attach debugger to the annotation processor. -J-ea enables assertions in the annotation processor.
        List<String> cmd = new ArrayList<>(List.of("javac", "-J-ea", "-processor",
                "com.doer.processor.DoerProcessor", "-cp", classpath, "-d", outputFolder));
        cmd.addAll(List.of(optionsAndFiles));
        return Processes.run(workDir, logName, TIMEOUT_SECONDS, cmd);
    }

    /** Runs the main class. The output is kept in java-out.txt and java-err.txt. */
    public static RunResult java(Path workDir, String classpath, String mainClass) throws Exception {
        return Processes.run(workDir, "java", TIMEOUT_SECONDS, List.of("java", "-cp", classpath, mainClass));
    }

    /** Runs mvn in batch mode. The output is kept in {@code <logName>-out.txt} and {@code <logName>-err.txt}. */
    public static RunResult mvn(Path workDir, String logName, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("mvn", "-B", "-Dmaven.repo.local=" + m2Repo));
        cmd.addAll(List.of(args));
        return Processes.run(workDir, logName, MVN_TIMEOUT_SECONDS, cmd);
    }

    private static String classpath(String... entries) {
        return String.join(File.pathSeparator, entries);
    }

    private static String jar(String groupPath, String version, String artifactId) {
        return m2Repo.resolve(groupPath).resolve(version).resolve(artifactId + "-" + version + ".jar").toString();
    }

    /** System property; also the default when it is set but blank (as an empty Maven property). */
    public static String property(String name, String defaultValue) {
        String value = System.getProperty(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
