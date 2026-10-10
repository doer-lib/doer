package com.doer.e2e;

import com.doer.testkit.Sources;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Transit Sims in one runtime: turns the Transit Sims sources into a Maven project in the work folder and says how to run the
 * built artifact in Docker. {@link E2eEnvironment} builds the project and runs it. See e2e-design.md.
 */
public interface E2eApp {

    /** Writes the complete Maven project of the runtime into the work folder. */
    void configure(Path workdir) throws Exception;

    /** How to run the artifact built in the work folder. The application must listen on 8080. */
    DockerRun dockerRun(Path workdir);

    /**
     * Arguments of {@code docker run} that differ between runtimes; name, network, port and {@code E2E_*} variables
     * are added by {@link E2eEnvironment}.
     *
     * @param options mounts of the built artifact and other options
     * @param image   image of the runtime, or a JRE image
     * @param command command in the container; empty for the command of the image
     */
    record DockerRun(List<String> options, String image, List<String> command) {
    }

    /** Copies a folder of test resources (for example {@code e2e/transitsims}) into the target folder. */
    default void copySources(String resourceFolder, Path target) throws IOException {
        Sources.copyResources(resourceFolder, target);
    }

    /** Writes the file (path is relative to the work folder). */
    default void writeFile(Path workdir, String path, String content) throws IOException {
        Sources.write(workdir, path, content);
    }
}
