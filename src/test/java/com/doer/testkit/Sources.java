package com.doer.testkit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Stream;

/** Source files of the code compiled by the tests: written from text blocks or copied from test resources. */
public final class Sources {

    private Sources() {
    }

    /** Writes the file (path is relative to the root folder), creating its folders. */
    public static void write(Path root, String path, String content) throws IOException {
        Path file = root.resolve(path);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    /** The folder of test resources (for example {@code e2e/transitsims}) on the file system. */
    public static Path resourceFolder(String resourceFolder) throws IOException {
        URL url = Sources.class.getClassLoader().getResource(resourceFolder);
        if (url == null) {
            throw new IOException("No test resource folder " + resourceFolder);
        }
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Copies a folder of test resources (for example {@code e2e/transitsims}) into the target folder, without its
     * documentation ({@code *.md}).
     */
    public static void copyResources(String resourceFolder, Path target) throws IOException {
        Path source = resourceFolder(resourceFolder);
        try (Stream<Path> files = Files.walk(source)) {
            files.filter(f -> Files.isRegularFile(f) && !f.toString().endsWith(".md")).forEach(file -> {
                try {
                    copy(file, target.resolve(source.relativize(file).toString()));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    /** Copies the file to the target file, creating its folders. */
    public static void copy(Path source, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    /** Paths of the .java files under the folder, relative to it. */
    public static List<String> javaFiles(Path folder) throws IOException {
        try (Stream<Path> files = Files.walk(folder)) {
            return files.filter(f -> f.toString().endsWith(".java"))
                    .map(f -> folder.relativize(f).toString())
                    .sorted()
                    .toList();
        }
    }

    /**
     * Source of TestDoerService.java: the generated service that works without database. It must be compiled
     * together with the doer classes. It makes public _load and _save, which DoerService.facilitateCoordinatedUpdate
     * uses to load and save task data by type.
     */
    public static final String TEST_DOER_SERVICE = """
            package demo.test;
            import com.doer.*;
            import com.doer.generated._GeneratedDoerService;
            public class TestDoerService extends _GeneratedDoerService {
                public TestDoerService() {
                    beanProviders.put(DoerService.class, () -> this);
                }
                @Override
                public boolean updateAndBumpVersion(Task task) {
                    return true;
                }
                @Override
                public TaskLog insertTaskLog(TaskLog log) {
                    return log;
                }
                @Override
                public Object _load(TaskSnapshot task, Class<?> type) throws Exception {
                    return super._load(task, type);
                }
                @Override
                public void _save(TaskSnapshot task, Class<?> type, Object data) throws Exception {
                    super._save(task, type, data);
                }
            }
            """;
}
