package com.doer.e2e;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** Transit Sims in WildFly: {@code ROOT.war} with its data source ({@code -ds.xml}), deployed by the deployment scanner. */
class WildflyApp extends JakartaEeServerApp {
    static final String WILDFLY_IMAGE = "quay.io/wildfly/wildfly:41.0.1.Final-jdk25";

    @Override
    String artifactId() {
        return "transitsims-wildfly";
    }

    /** JNDI name of the data source the server defines. */
    String dataSourceJndiName() {
        return "java:jboss/datasources/TransitSims";
    }

    @Override
    String producers() {
        return jndiProducers(dataSourceJndiName());
    }

    @Override
    void configureServer(Path workdir) throws IOException {
        // The driver of the WAR is registered as ROOT.war_<driver class>_<major>_<minor>
        String[] version = POSTGRESQL_VERSION.split("\\.");
        String driver = "ROOT.war_org.postgresql.Driver_" + version[0] + "_" + version[1];
        writeFile(workdir, "src/main/webapp/WEB-INF/transitsims-ds.xml", """
                <?xml version="1.0" encoding="UTF-8"?>
                <datasources xmlns="http://www.jboss.org/ironjacamar/schema">
                    <!-- The defaults are for a run from the IDE (doer.e2e.runtime=external) -->
                    <datasource jndi-name="%s" pool-name="TransitSims">
                        <connection-url>${e2e.db.url:jdbc:postgresql://localhost:5432/doer}</connection-url>
                        <driver>%s</driver>
                        <pool>
                            <max-pool-size>4</max-pool-size>
                        </pool>
                        <security>
                            <user-name>${e2e.db.user:doer}</user-name>
                            <password>${e2e.db.password:doer}</password>
                        </security>
                    </datasource>
                </datasources>
                """.formatted(dataSourceJndiName(), driver));
        // The pool of the ManagedExecutorService has as many threads as processors (4 on a runner of GitHub Actions)
        // and an unbounded queue: Doer would run fewer tasks at once than its limits. 200, as the pool of Quarkus.
        writeFile(workdir, "src/main/wildfly/executor.cli", """
                embed-server --std-out=echo
                /subsystem=ee/managed-executor-service=default:write-attribute(name=core-threads, value=200)
                stop-embedded-server
                """);
        // RESTEasy of WildFly writes JSON with Jackson; Transit Sims uses JSON-B
        writeFile(workdir, "src/main/webapp/WEB-INF/web.xml", """
                <?xml version="1.0" encoding="UTF-8"?>
                <web-app xmlns="https://jakarta.ee/xml/ns/jakartaee" version="6.1">
                    <context-param>
                        <param-name>resteasy.preferJacksonOverJsonB</param-name>
                        <param-value>false</param-value>
                    </context-param>
                </web-app>
                """);
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
    public DockerRun dockerRun(Path workdir) {
        return new DockerRun(List.of("-v", workdir.resolve("target/ROOT.war")
                + ":/opt/jboss/wildfly/standalone/deployments/ROOT.war:ro",
                "-v", workdir.resolve("src/main/wildfly/executor.cli") + ":/opt/jboss/e2e/executor.cli:ro"),
                WILDFLY_IMAGE, List.of("sh", "-c", START));
    }
}
