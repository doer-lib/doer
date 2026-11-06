package com.doer.e2e;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testcontainers.DockerClientFactory;

/**
 * One node of the application: a {@code docker run --rm} process of the image of the application. Its stdout and
 * stderr are appended to {@code app-<node>-out.txt} and {@code app-<node>-err.txt} of the work folder, with a separator
 * line at every start and stop, and every command to {@code app-<node>-cmd.txt}, for all starts in this run. The host
 * port is kept across restarts. When the node is ready and before a stop the resources the container used are
 * measured, see {@link ContainerUsage}. Before a stop the files the container added, changed and deleted
 * ({@code docker diff}), with the sizes of the added and changed ones, are appended to {@code app-<node>-diff.txt}:
 * what the runtime writes to its disk.
 */
class AppNode {
    static final int STOP_TIMEOUT_SECONDS = 60;
    /**
     * CPUs of a container, as on a runner of GitHub Actions: a pool of a runtime sized by the number of processors is
     * then as small as in CI.
     */
    static final String CPUS = "4";
    static final int DOCKER_COMMAND_TIMEOUT_SECONDS = 30;
    /** A line of {@code du -sk}: the size in kB, a tab and the path. */
    private static final Pattern DU_LINE = Pattern.compile("(\\d+)\t(.+)");

    final int node;
    final int port;
    final Path out;
    final Path err;
    final Path cmd;
    final Path diff;
    /** The resources used by the containers of the node, one per stopped start. */
    final List<ContainerUsage> usages = new ArrayList<>();
    private final String runtime;
    private final String network;
    private final Map<String, String> env;
    private final String image;
    private Process process;
    private String name;
    private int starts;
    /** The usage until the node was ready, for the current start; null when not measured. */
    private ContainerUsage startup;

    AppNode(String runtime, int node, Path workdir, String network, Map<String, String> env, String image) {
        this.runtime = runtime;
        this.node = node;
        this.network = network;
        this.env = env;
        this.image = image;
        this.port = freePort();
        this.out = workdir.resolve("app-" + node + "-out.txt");
        this.err = workdir.resolve("app-" + node + "-err.txt");
        this.cmd = workdir.resolve("app-" + node + "-cmd.txt");
        this.diff = workdir.resolve("app-" + node + "-diff.txt");
        // The work folder is kept with doer.e2e.skip-build: the files keep the starts of this run only
        try {
            Files.deleteIfExists(out);
            Files.deleteIfExists(err);
            Files.deleteIfExists(cmd);
            Files.deleteIfExists(diff);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    String baseUrl() {
        return "http://127.0.0.1:" + port;
    }

    /** Starts {@code docker run}; does not wait until the application is ready. */
    void start() throws IOException {
        if (process != null) {
            throw new IllegalStateException("Node " + node + " is already started");
        }
        starts++;
        startup = null;
        name = "transitsims-" + runtime + "-" + node + "-" + starts;
        List<String> command = new ArrayList<>(List.of("docker", "run", "--rm", "--name", name,
                "--network", network, "-p", "127.0.0.1:" + port + ":8080", "--cpus", CPUS));
        // Ryuk removes the container when the test JVM ends, as it does with Testcontainers' own containers
        DockerClientFactory.DEFAULT_LABELS.forEach((k, v) -> command.addAll(List.of("--label", k + "=" + v)));
        command.addAll(List.of("--label",
                DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL + "=" + DockerClientFactory.SESSION_ID));
        env.forEach((k, v) -> command.addAll(List.of("-e", k + "=" + v)));
        command.add(image);

        separator("node " + node + " started: " + name);
        Files.writeString(cmd, String.join(" ", command) + "\n", CREATE, APPEND);
        process = new ProcessBuilder(command)
                .redirectOutput(Redirect.appendTo(out.toFile()))
                .redirectError(Redirect.appendTo(err.toFile()))
                .start();
    }

    /**
     * Stops the node with SIGTERM: {@code docker run} passes it to the application, and {@code --rm} removes the
     * container.
     *
     * @return the exit code of {@code docker run}, which is the exit code of the application
     */
    int stop() throws IOException, InterruptedException {
        if (process == null) {
            throw new IllegalStateException("Node " + node + " is not started");
        }
        String usage = measureUsage();
        // After the measure: du runs in the container
        writeDiff();
        long t0 = System.currentTimeMillis();
        process.destroy();
        boolean exited = process.waitFor(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        double seconds = (System.currentTimeMillis() - t0) / 1000.0;
        if (!exited) {
            separator(String.format(Locale.ROOT, "node %s did not stop in %.1f s%s", node, seconds, usage));
            process.destroyForcibly();
            process = null;
            throw new IllegalStateException("Node " + node + " did not stop in " + STOP_TIMEOUT_SECONDS + " s");
        }
        int exitCode = process.exitValue();
        process = null;
        separator(String.format(Locale.ROOT, "node %s stopped in %.1f s, exit code %s%s", node, seconds, exitCode,
                usage));
        return exitCode;
    }

    /**
     * Measures the resources the container used until the node was ready; called when it is ready. It is reported with
     * the usage of the stop, see {@link #measureUsage()}: the log of a start has no line of its own for it. A failure
     * does not fail the test. Docker takes about a second to answer.
     */
    void measureStartup() {
        if (process == null) {
            throw new IllegalStateException("Node " + node + " is not started");
        }
        if (exitCode() != null) {
            return;
        }
        try {
            startup = ContainerUsage.measure(name, node);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            // Not measured: the usage of the stop has no startup
        }
    }

    /**
     * Measures the resources the running container used, with the usage until it was ready, into {@link #usages}. A
     * failure does not fail the test: the measure is for the test summary only.
     *
     * @return the usage, or the failure, for the separator line of the stop
     */
    private String measureUsage() {
        if (exitCode() != null) {
            return "";
        }
        try {
            ContainerUsage usage = ContainerUsage.measure(name, node).withStartup(startup);
            usages.add(usage);
            return ", used " + usage.summary();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ", usage not measured: interrupted";
        } catch (RuntimeException e) {
            return ", usage not measured: " + e;
        }
    }

    /**
     * Appends the files the running container added (A), changed (C) and deleted (D) to {@link #diff}: first the sizes
     * of the added trees and the changed files ({@code du -sk} in the container, largest first, without the mounts in
     * them: a mounted artifact is not written), then the whole {@code docker diff}. The sizes are of the files now, not
     * the bytes written: a file written over or deleted is not counted. A failure does not fail the test.
     */
    private void writeDiff() throws IOException {
        if (exitCode() != null) {
            return;
        }
        StringBuilder text = new StringBuilder();
        try {
            List<String> entries = docker(List.of("docker", "diff", name)).lines().filter(l -> !l.isBlank()).toList();
            NavigableSet<String> paths = new TreeSet<>();
            NavigableSet<String> added = new TreeSet<>();
            int[] counts = new int[3];
            for (String entry : entries) {
                String path = entry.substring(2);
                paths.add(path);
                switch (entry.charAt(0)) {
                    case 'A' -> {
                        added.add(path);
                        counts[0]++;
                    }
                    case 'C' -> counts[1]++;
                    default -> counts[2]++;
                }
            }
            // The roots of the added trees, and the changed paths with no change below them: the changed files
            List<String> measured = new ArrayList<>();
            for (String entry : entries) {
                String path = entry.substring(2);
                String below = paths.higher(path);
                boolean leaf = below == null || !below.startsWith(path + "/");
                if (entry.charAt(0) == 'A' ? !added.contains(parent(path)) : entry.charAt(0) == 'C' && leaf) {
                    measured.add(path);
                }
            }
            text.append(String.format(Locale.ROOT, "%d added, %d changed, %d deleted%n", counts[0], counts[1],
                    counts[2]));
            // du in the image may not skip a mounted file (-x): the size of a mount is taken off the path it is in
            List<String> mounts = docker(List.of("docker", "inspect", "-f",
                    "{{range .Mounts}}{{println .Destination}}{{end}}", name)).lines()
                    .filter(l -> !l.isBlank()).toList();
            measured.removeIf(path -> mounts.stream().anyMatch(m -> isUnder(path, m)));
            List<String> mountsIn = mounts.stream()
                    .filter(m -> measured.stream().anyMatch(path -> isUnder(m, path))).toList();
            if (!measured.isEmpty()) {
                Map<String, Long> kb = new TreeMap<>();
                List<String> other = new ArrayList<>();
                // The mounts in a call of their own: du counts a file once, in the first path it is in
                for (List<String> group : List.of(measured, mountsIn)) {
                    if (group.isEmpty()) {
                        continue;
                    }
                    List<String> du = new ArrayList<>(List.of("docker", "exec", name, "du", "-sk", "--"));
                    du.addAll(group);
                    for (String line : docker(du).lines().toList()) {
                        Matcher m = DU_LINE.matcher(line);
                        if (m.matches()) {
                            kb.put(m.group(2), Long.parseLong(m.group(1)));
                        } else {
                            other.add(line);
                        }
                    }
                }
                Map<String, Long> sizes = new TreeMap<>();
                for (String path : measured) {
                    if (kb.containsKey(path)) {
                        sizes.put(path, kb.get(path) - mountsIn.stream().filter(m -> isUnder(m, path))
                                .mapToLong(m -> kb.getOrDefault(m, 0L)).sum());
                    }
                }
                text.append("du -sk of the added trees and the changed files, without mounts, ")
                        .append(sizes.values().stream().mapToLong(Long::longValue).sum()).append(" kB:\n");
                sizes.entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                        .forEach(e -> text.append(e.getValue()).append('\t').append(e.getKey()).append('\n'));
                other.forEach(line -> text.append(line).append('\n'));
            }
            text.append("docker diff:\n");
            entries.forEach(line -> text.append(line).append('\n'));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            text.append("not written: interrupted\n");
        } catch (IOException | RuntimeException e) {
            text.append("not written: ").append(e).append('\n');
        }
        String header = "==== " + Instant.now().truncatedTo(ChronoUnit.MILLIS) + " node " + node + " before stop: "
                + name + " ====\n";
        Files.writeString(diff, header + text, CREATE, APPEND);
    }

    private static String parent(String path) {
        int slash = path.lastIndexOf('/');
        return slash <= 0 ? "/" : path.substring(0, slash);
    }

    /** True when the path is the folder or in it. */
    private static boolean isUnder(String path, String folder) {
        return path.equals(folder) || path.startsWith(folder.endsWith("/") ? folder : folder + "/");
    }

    /** Runs a docker command; its stdout and stderr, together. */
    private static String docker(List<String> command) throws IOException, InterruptedException {
        Path output = Files.createTempFile("e2e-docker-", ".txt");
        try {
            Process p = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
            if (!p.waitFor(DOCKER_COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                throw new IOException("Not finished in " + DOCKER_COMMAND_TIMEOUT_SECONDS + " s: "
                        + String.join(" ", command));
            }
            return Files.readString(output);
        } finally {
            Files.deleteIfExists(output);
        }
    }

    /**
     * Sends SIGQUIT to the application: a HotSpot JVM writes the dump of its threads to stdout, into the log of the node;
     * OpenJ9 (Open Liberty) writes a javacore file in the container instead.
     */
    void threadDump() throws IOException, InterruptedException {
        if (process == null) {
            throw new IllegalStateException("Node " + node + " is not started");
        }
        separator("node " + node + " thread dump");
        new ProcessBuilder("docker", "kill", "--signal=QUIT", name).inheritIO().start().waitFor();
    }

    /** The exit code if {@code docker run} has exited by itself, or null. */
    Integer exitCode() {
        return process != null && !process.isAlive() ? process.exitValue() : null;
    }

    /** For the shutdown hook: stops the node, if it is running, as {@link #stop()} does. */
    void destroy() {
        if (process != null) {
            try {
                stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Appends a separator line with the timestamp to both log files; in UTC, as the logs of the containers. On a line of
     * its own: a log may end without a line break (WildFly ends it with an ANSI reset).
     */
    private void separator(String text) throws IOException {
        String line = "==== " + Instant.now().truncatedTo(ChronoUnit.MILLIS) + " " + text + " ====\n";
        for (Path log : List.of(out, err)) {
            Files.writeString(log, (endsWithLineBreak(log) ? "" : "\n") + line, CREATE, APPEND);
        }
    }

    private static boolean endsWithLineBreak(Path log) throws IOException {
        if (!Files.exists(log) || Files.size(log) == 0) {
            return true;
        }
        try (RandomAccessFile file = new RandomAccessFile(log.toFile(), "r")) {
            file.seek(file.length() - 1);
            return file.read() == '\n';
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
