package com.doer.e2e.runtime;

import com.doer.testkit.Sources;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Transit Sims in one runtime: turns the Transit Sims sources into a Maven project in the work folder and says how to
 * run the built artifact in Docker. What the runtime needs (pom.xml, its configuration files and sources) is in the
 * folder of test resources {@code e2e-<runtime>}. {@link com.doer.e2e.E2eEnvironment} builds the project and runs it.
 * See src/test/README.md.
 */
public interface E2eApp {
    /** The image of the runtimes that run an executable jar. */
    String JRE_IMAGE = "eclipse-temurin:25-jre";

    /** The app of the runtime given by {@code -Ddoer.e2e.runtime}, for example {@code quarkus}. */
    static E2eApp of(String runtime) {
        return switch (runtime) {
            case "quarkus" -> new QuarkusApp();
            case "spring-boot" -> new SpringBootApp();
            case "wildfly" -> new WildflyApp();
            case "open-liberty" -> new OpenLibertyApp();
            case "jetty" -> new JettyApp();
            default -> throw new IllegalArgumentException("Unknown doer.e2e.runtime: " + runtime);
        };
    }

    /** The folder of test resources with the files of the runtime, for example {@code e2e-wildfly}. */
    String customSourcesFolder();

    /** Writes the complete Maven project of the runtime into the work folder. */
    void configure(Path workdir) throws Exception;

    /** How to run the artifact built in the work folder. The application must listen on 8080. */
    DockerRunSpec dockerRun(Path workdir);

    /** Copies a folder of test resources (for example {@code e2e/transitsims}) into the target folder. */
    default void copySources(String resourceFolder, Path target) throws IOException {
        Sources.copyResources(resourceFolder, target);
    }

    /**
     * Copies the file of {@link #customSourcesFolder()} (for example {@code pom.xml}) into the target folder, or, as
     * {@code cp}, to the target file when the target has the name of the file. The files are not renamed.
     */
    default void copyCustomSource(String name, Path target) throws IOException {
        Path file = target.getFileName().toString().equals(name) ? target : target.resolve(name);
        Sources.copy(Sources.resourceFolder(customSourcesFolder()).resolve(name), file);
    }
}
