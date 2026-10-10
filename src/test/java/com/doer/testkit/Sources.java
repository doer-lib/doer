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

    /** Copies a folder of test resources (for example {@code e2e/transitsims}) into the target folder. */
    public static void copyResources(String resourceFolder, Path target) throws IOException {
        URL url = Sources.class.getClassLoader().getResource(resourceFolder);
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
}
