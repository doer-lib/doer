package com.doer.e2e;

import static com.doer.testkit.Toolchain.property;

import com.doer.testkit.Postgres;
import com.doer.testkit.Processes;
import com.doer.testkit.RunResult;
import com.doer.testkit.Toolchain;
import com.doer.testkit.Workspaces;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.restassured.path.json.JsonPath;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * CarWash running in the runtime given by {@code -Ddoer.e2e.runtime}, with Postgres and WireMock, for the
 * {@code *E2E} test classes ({@code @ExtendWith(E2eEnvironment.class)}). One environment per JVM: the first test
 * class starts it, and it is stopped when the JVM ends. See e2e-design.md.
 *
 * <pre>
 * target/e2e/&lt;runtime&gt;/                    the work folder: a complete Maven project of CarWash in the runtime
 *     mvn-package-*.txt                     command and output of the build
 *     docker-pull-*.txt                     the same for docker pull of the application image
 *     app-&lt;node&gt;-out.txt, app-&lt;node&gt;-err.txt  stdout and stderr of the application, all starts of the node
 *     app-&lt;node&gt;-cmd.txt                    docker run command of every start
 * </pre>
 *
 * Properties:
 * <ul>
 * <li>{@code doer.e2e.runtime} — {@code quarkus} (when not set), ..., or {@code external}: test an application that
 * is already running, at {@code doer.e2e.base-url}, {@code doer.e2e.jdbc-url} and {@code doer.e2e.wiremock-url};</li>
 * <li>{@code doer.e2e.skip-build=true} — reuse the work folder of the previous run.</li>
 * </ul>
 */
public class E2eEnvironment implements BeforeAllCallback {
    static final String POSTGRES_IMAGE = Postgres.IMAGE;
    static final String WIREMOCK_IMAGE = "wiremock/wiremock:3.13.2";
    static final int DOCKER_PULL_TIMEOUT_SECONDS = 600;
    static final int READY_TIMEOUT_SECONDS = 120;
    static final String EXTERNAL = "external";

    public static final String runtime = property("doer.e2e.runtime", "quarkus");
    static final boolean skipBuild = Boolean.parseBoolean(property("doer.e2e.skip-build", "false"));
    static final Path workdir = Path.of("target", "e2e", runtime).toAbsolutePath();

    private static boolean started;
    private static Throwable startFailure;
    private static DataSource dataSource;
    private static Network network;
    private static E2eApp.DockerRun dockerRun;
    private static Map<String, String> appEnv;
    private static final Map<Integer, AppNode> nodes = new TreeMap<>();

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        start();
    }

    static synchronized void start() throws Exception {
        if (startFailure != null) {
            throw new IllegalStateException("E2E environment failed to start for a previous test class", startFailure);
        }
        if (started) {
            return;
        }
        try {
            if (isExternal()) {
                startExternal();
            } else {
                startInDocker();
            }
            started = true;
        } catch (Throwable e) {
            startFailure = e;
            throw e;
        }
    }

    /** True for {@code doer.e2e.runtime=external}: an application that is already running, not in Docker. */
    static boolean isExternal() {
        return EXTERNAL.equals(runtime);
    }

    /** The {@link E2eApp} of the runtime. */
    static E2eApp app(String runtime) {
        return switch (runtime) {
            case "quarkus" -> new QuarkusApp();
            default -> throw new IllegalArgumentException("Unknown doer.e2e.runtime: " + runtime);
        };
    }

    private static void startInDocker() throws Exception {
        E2eApp app = app(runtime);
        Runtime.getRuntime().addShutdownHook(new Thread(E2eEnvironment::destroyNodes));

        long t0 = System.currentTimeMillis();
        network = Network.newNetwork();
        PostgreSQLContainer postgres = new PostgreSQLContainer(POSTGRES_IMAGE)
                .withDatabaseName("doer")
                .withUsername("doer")
                .withPassword("doer")
                .withNetwork(network)
                .withNetworkAliases("db");
        GenericContainer<?> wiremock = new GenericContainer<>(WIREMOCK_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("wiremock")
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/__admin/mappings"));
        // Postgres and WireMock start while the application is built
        CompletableFuture<Void> containers = Startables.deepStart(postgres, wiremock);

        if (skipBuild) {
            if (!Files.isDirectory(workdir.resolve("target"))) {
                throw new IllegalStateException("Nothing to reuse with doer.e2e.skip-build=true: " + workdir
                        + " is not built. Run once without doer.e2e.skip-build.");
            }
            System.out.printf("✔ Build skipped, reusing %s%n", workdir);
        } else {
            Workspaces.recreate(workdir);
            app.configure(workdir);
            RunResult build = Toolchain.mvn(workdir, "mvn-package", "package");
            build.assertStatus(0);
            System.out.printf("✔ CarWash for %s built in %s ms: %s%n", runtime, build.runMilliseconds(), workdir);
        }
        checkMigration("V1__create_doer_schema.sql", "CreateSchema.sql");
        checkMigration("V2__create_doer_indexes.sql", "CreateIndexes.sql");

        dockerRun = app.dockerRun(workdir);
        RunResult pull = Processes.run(workdir, "docker-pull", DOCKER_PULL_TIMEOUT_SECONDS,
                List.of("docker", "pull", dockerRun.image()));
        pull.assertStatus(0);

        containers.join();
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] { postgres.getHost() });
        ds.setPortNumbers(new int[] { postgres.getMappedPort(5432) });
        ds.setDatabaseName("doer");
        ds.setUser("doer");
        ds.setPassword("doer");
        dataSource = ds;
        WireMock.configureFor(wiremock.getHost(), wiremock.getMappedPort(8080));
        System.out.printf("✔ Postgres and WireMock started, image %s pulled, in %s ms%n", dockerRun.image(),
                System.currentTimeMillis() - t0);

        appEnv = new LinkedHashMap<>();
        appEnv.put("E2E_RUNTIME", runtime);
        appEnv.put("E2E_DB_URL", "jdbc:postgresql://db:5432/doer");
        appEnv.put("E2E_DB_USER", "doer");
        appEnv.put("E2E_DB_PASSWORD", "doer");
        startNode(1);
        RestAssured.baseURI = nodes.get(1).baseUrl();
    }

    private static void startExternal() throws Exception {
        String baseUrl = property("doer.e2e.base-url", "http://localhost:8080");
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL(property("doer.e2e.jdbc-url", "jdbc:postgresql://localhost:5432/doer?user=doer&password=doer"));
        dataSource = ds;
        URI wiremockUrl = URI.create(property("doer.e2e.wiremock-url", "http://localhost:8089"));
        WireMock.configureFor(wiremockUrl.getScheme(), wiremockUrl.getHost(), wiremockUrl.getPort());
        RestAssured.baseURI = baseUrl;
        waitReady(baseUrl, null);
        System.out.printf("✔ External application is ready at %s%n", baseUrl);
    }

    /**
     * Starts the node (a new one, or one stopped by {@link #stopNode}) and waits until it is ready. Its log files get
     * a separator line, and the output of the new start is appended to them.
     */
    public static synchronized void startNode(int node) throws Exception {
        requireDocker("startNode");
        AppNode appNode = nodes.computeIfAbsent(node,
                n -> new AppNode(runtime, n, workdir, network.getId(), appEnv, dockerRun));
        long t0 = System.currentTimeMillis();
        appNode.start();
        waitReady(appNode.baseUrl(), appNode);
        System.out.printf("✔ Node %s of CarWash started in %s ms at %s%n", node, System.currentTimeMillis() - t0,
                appNode.baseUrl());
    }

    /**
     * Stops the node with SIGTERM and waits until it exits.
     *
     * @return the exit code of the application
     */
    public static synchronized int stopNode(int node) throws Exception {
        requireDocker("stopNode");
        return appNode(node).stop();
    }

    /** Base URL of the node, for example {@code http://127.0.0.1:32801}; RestAssured is set to the one of node 1. */
    public static String baseUrl(int node) {
        requireDocker("baseUrl");
        return appNode(node).baseUrl();
    }

    /** DataSource of the e2e Postgres. */
    public static DataSource dataSource() {
        return dataSource;
    }

    /** The application log (stdout) of the node, all its starts. */
    public static String appLog(int node) throws IOException {
        requireDocker("appLog");
        return Files.readString(appNode(node).out);
    }

    /**
     * Fails when the generated SQL differs from the committed migration: the committed one must be a copy of what the
     * processor generates for CarWash.
     */
    private static void checkMigration(String migration, String generatedFile) throws IOException {
        Path generated = workdir.resolve("target/classes/com/doer/generated").resolve(generatedFile);
        String committed;
        try (InputStream in = E2eEnvironment.class.getResourceAsStream("/e2e/db/migration/" + migration)) {
            committed = new String(Objects.requireNonNull(in, migration).readAllBytes(), StandardCharsets.UTF_8);
        }
        if (!committed.equals(Files.readString(generated))) {
            throw new AssertionError("src/test/resources/e2e/db/migration/" + migration
                    + " differs from the SQL generated for CarWash. Copy it:\n    cp " + generated
                    + " src/test/resources/e2e/db/migration/" + migration);
        }
    }

    /**
     * Waits until {@code GET /api/validation/info} answers with the runtime (any runtime when null). Fails when the
     * node exits before, or on the timeout.
     */
    private static void waitReady(String baseUrl, AppNode node) throws Exception {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/api/validation/info"))
                .timeout(Duration.ofSeconds(5))
                .build();
        String expected = node == null ? null : runtime;
        long deadline = System.currentTimeMillis() + READY_TIMEOUT_SECONDS * 1000L;
        String last = "no answer";
        while (true) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    String answered = JsonPath.from(response.body()).getString("runtime");
                    if (expected == null || expected.equals(answered)) {
                        return;
                    }
                }
                last = response.statusCode() + " " + response.body();
            } catch (IOException e) {
                last = e.toString();
            }
            if (node != null && node.exitCode() != null) {
                throw new AssertionError("Node " + node.node + " exited with code " + node.exitCode()
                        + " before it was ready" + logTail(node));
            }
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("No " + baseUrl + "/api/validation/info with runtime " + expected + " in "
                        + READY_TIMEOUT_SECONDS + " s, last: " + last + (node == null ? "" : logTail(node)));
            }
            Thread.sleep(500);
        }
    }

    private static String logTail(AppNode node) throws IOException {
        return "\n----- " + node.out + " -----\n" + tail(node.out)
                + "----- " + node.err + " -----\n" + tail(node.err)
                + "------------------";
    }

    private static String tail(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);
        return String.join("\n", lines.subList(Math.max(0, lines.size() - 50), lines.size())) + "\n";
    }

    private static AppNode appNode(int node) {
        return Objects.requireNonNull(nodes.get(node), "No node " + node);
    }

    private static void requireDocker(String method) {
        if (isExternal()) {
            throw new UnsupportedOperationException(method + " is not available with doer.e2e.runtime=external");
        }
    }

    private static synchronized void destroyNodes() {
        nodes.values().forEach(AppNode::destroy);
    }
}
