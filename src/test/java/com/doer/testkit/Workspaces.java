package com.doer.testkit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AnnotatedElementContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDirFactory;

/**
 * Workspace folders of the tests, by the test class and method. The folder is cleared when it is created and kept
 * after the test, so after a failure it has the sources, the compiled and generated files and the output of every
 * command:
 *
 * <pre>
 * &#64;TempDir(factory = Workspaces.class, cleanup = CleanupMode.NEVER)
 * Path workspace;
 *
 * target/it-test-workspaces/&lt;test class&gt;/                a static field (set before @BeforeAll)
 * target/it-test-workspaces/&lt;test class&gt;/&lt;test method&gt;/  an instance field
 * </pre>
 */
public class Workspaces implements TempDirFactory {
    public static final Path ROOT = Path.of("target", "it-test-workspaces").toAbsolutePath();

    @Override
    public Path createTempDirectory(AnnotatedElementContext elementContext, ExtensionContext extensionContext)
            throws IOException {
        Path folder = ROOT.resolve(extensionContext.getRequiredTestClass().getSimpleName());
        if (extensionContext.getTestMethod().isPresent()) {
            folder = folder.resolve(extensionContext.getRequiredTestMethod().getName());
        }
        return recreate(folder);
    }

    /** Deletes the folder with all its content and creates it empty. */
    public static Path recreate(Path folder) throws IOException {
        deleteRecursively(folder);
        Files.createDirectories(folder);
        return folder;
    }

    public static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> files = Files.walk(path)) {
            files.sorted(Comparator.reverseOrder()).forEach(file -> {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}
