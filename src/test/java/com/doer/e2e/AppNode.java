package com.doer.e2e;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
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
import java.util.concurrent.TimeUnit;
import org.testcontainers.DockerClientFactory;

/**
 * One node of the application: a {@code docker run --rm} process. Its stdout and stderr are appended to
 * {@code app-<node>-out.txt} and {@code app-<node>-err.txt} of the work folder, with a separator line at every start
 * and stop, and every command to {@code app-<node>-cmd.txt}, for all starts in this run. The host port is kept
 * across restarts.
 */
class AppNode {
    static final int STOP_TIMEOUT_SECONDS = 60;

    final int node;
    final int port;
    final Path out;
    final Path err;
    final Path cmd;
    private final String runtime;
    private final String network;
    private final Map<String, String> env;
    private final E2eApp.DockerRun dockerRun;
    private Process process;
    private int starts;

    AppNode(String runtime, int node, Path workdir, String network, Map<String, String> env,
            E2eApp.DockerRun dockerRun) {
        this.runtime = runtime;
        this.node = node;
        this.network = network;
        this.env = env;
        this.dockerRun = dockerRun;
        this.port = freePort();
        this.out = workdir.resolve("app-" + node + "-out.txt");
        this.err = workdir.resolve("app-" + node + "-err.txt");
        this.cmd = workdir.resolve("app-" + node + "-cmd.txt");
        // The work folder is kept with doer.e2e.skip-build: the files keep the starts of this run only
        try {
            Files.deleteIfExists(out);
            Files.deleteIfExists(err);
            Files.deleteIfExists(cmd);
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
        String name = "transitsims-" + runtime + "-" + node + "-" + starts;
        List<String> command = new ArrayList<>(List.of("docker", "run", "--rm", "--name", name,
                "--network", network, "-p", "127.0.0.1:" + port + ":8080"));
        // Ryuk removes the container when the test JVM ends, as it does with Testcontainers' own containers
        DockerClientFactory.DEFAULT_LABELS.forEach((k, v) -> command.addAll(List.of("--label", k + "=" + v)));
        command.addAll(List.of("--label",
                DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL + "=" + DockerClientFactory.SESSION_ID));
        env.forEach((k, v) -> command.addAll(List.of("-e", k + "=" + v)));
        command.addAll(dockerRun.options());
        command.add(dockerRun.image());
        command.addAll(dockerRun.command());

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
        long t0 = System.currentTimeMillis();
        process.destroy();
        boolean exited = process.waitFor(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        double seconds = (System.currentTimeMillis() - t0) / 1000.0;
        if (!exited) {
            separator(String.format(Locale.ROOT, "node %s did not stop in %.1f s", node, seconds));
            process.destroyForcibly();
            process = null;
            throw new IllegalStateException("Node " + node + " did not stop in " + STOP_TIMEOUT_SECONDS + " s");
        }
        int exitCode = process.exitValue();
        process = null;
        separator(String.format(Locale.ROOT, "node %s stopped in %.1f s, exit code %s", node, seconds, exitCode));
        return exitCode;
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

    /** Appends a separator line with the timestamp to both log files; in UTC, as the logs of the containers. */
    private void separator(String text) throws IOException {
        String line = "==== " + Instant.now().truncatedTo(ChronoUnit.MILLIS) + " " + text + " ====\n";
        Files.writeString(out, line, CREATE, APPEND);
        Files.writeString(err, line, CREATE, APPEND);
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
