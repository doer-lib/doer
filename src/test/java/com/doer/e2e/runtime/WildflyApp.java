package com.doer.e2e.runtime;

import java.nio.file.Path;
import java.util.List;

/** Transit Sims in WildFly: {@code ROOT.war} with its data source ({@code -ds.xml}), deployed by the deployment scanner. */
class WildflyApp implements E2eApp {
    static final String WILDFLY_IMAGE = "quay.io/wildfly/wildfly:41.0.1.Final-jdk25";

    @Override
    public String customSourcesFolder() {
        return "e2e-wildfly";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path webInf = workdir.resolve("src/main/webapp/WEB-INF");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", workdir.resolve("src/main/resources/db/migration"));
        copyCustomSource("pom.xml", workdir.resolve("pom.xml"));
        // The DataSource and the ManagedExecutorService of the server as CDI beans, and BeanKindSingleton
        copyCustomSource("Producers.java", sources.resolve("Producers.java"));
        // Flyway at start, before DoerLifecycle starts Doer
        copyCustomSource("FlywayMigration.java", sources.resolve("FlywayMigration.java"));
        // The data source with system properties: WildFly does not read environment variables there
        copyCustomSource("transitsims-ds.xml", webInf.resolve("transitsims-ds.xml"));
        // RESTEasy of WildFly writes JSON with Jackson; Transit Sims uses JSON-B
        copyCustomSource("web.xml", webInf.resolve("web.xml"));
        // The pool of the ManagedExecutorService has as many threads as processors (4 on a runner of GitHub Actions)
        // and an unbounded queue: Doer would run fewer tasks at once than its limits. 200, as the pool of Quarkus.
        copyCustomSource("executor.cli", workdir.resolve("src/main/wildfly/executor.cli"));
    }

    /**
     * {@code standalone.sh} with {@code exec}. With {@code LAUNCH_JBOSS_IN_BACKGROUND}, set in the image, the script runs
     * Java in the background and turns its exit code after SIGTERM (143) into 0; in the foreground it runs Java without
     * {@code exec}, and the signals of the container do not reach Java. So the container runs a copy of the script that
     * starts Java with {@code exec} in the foreground: Java gets the signals, and its exit code is the one of the container.
     * The data source gets the {@code E2E_DB_*} variables as system properties. Before the start, {@code executor.cli}
     * configures the server with the embedded server of {@code jboss-cli.sh}.
     */
    static final String START = """
            "$JBOSS_HOME/bin/jboss-cli.sh" --file=/opt/jboss/e2e/executor.cli
            sed 's/eval \\\\"\\$JAVA\\\\" -D/eval exec \\\\"$JAVA\\\\" -D/' "$JBOSS_HOME/bin/standalone.sh" > "$JBOSS_HOME/bin/standalone-exec.sh"
            LAUNCH_JBOSS_IN_BACKGROUND= exec sh "$JBOSS_HOME/bin/standalone-exec.sh" -b 0.0.0.0 \\
                -De2e.db.url="$E2E_DB_URL" -De2e.db.user="$E2E_DB_USER" -De2e.db.password="$E2E_DB_PASSWORD"
            """;

    @Override
    public DockerRunSpec dockerRun(Path workdir) {
        return new DockerRunSpec(List.of("-v", workdir.resolve("target/ROOT.war")
                + ":/opt/jboss/wildfly/standalone/deployments/ROOT.war:ro",
                "-v", workdir.resolve("src/main/wildfly/executor.cli") + ":/opt/jboss/e2e/executor.cli:ro"),
                WILDFLY_IMAGE, List.of("sh", "-c", START));
    }
}
