package com.doer.e2e;

import com.doer.GeneratorTestBase;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Stream;

/**
 * CarWash in one runtime: turns the CarWash sources into a Maven project in the work folder and says how to run the
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

    /** Copies a folder of test resources (for example {@code e2e/carwash}) into the target folder. */
    default void copySources(String resourceFolder, Path target) throws IOException {
        URL url = E2eApp.class.getClassLoader().getResource(resourceFolder);
        if (url == null) {
            throw new IOException("No test resource folder " + resourceFolder);
        }
        Path source;
        try {
            source = Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        try (Stream<Path> files = Files.walk(source)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                Path copy = target.resolve(source.relativize(file).toString());
                try {
                    Files.createDirectories(copy.getParent());
                    Files.copy(file, copy, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    /** Writes the file (path is relative to the work folder). */
    default void writeFile(Path workdir, String path, String content) throws IOException {
        GeneratorTestBase.writeSource(workdir, path, content);
    }
}
