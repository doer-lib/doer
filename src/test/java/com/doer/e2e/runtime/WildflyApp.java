package com.doer.e2e.runtime;

import java.nio.file.Path;

/**
 * Transit Sims in WildFly: {@code ROOT.war} with its data source ({@code -ds.xml}), unpacked, deployed by the
 * deployment scanner.
 */
class WildflyApp implements E2eApp {

    @Override
    public String customSourcesFolder() {
        return "e2e-wildfly";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path migrations = workdir.resolve("src/main/resources/db/migration");
        Path webInf = workdir.resolve("src/main/webapp/WEB-INF");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", migrations);
        copyCustomSource("pom.xml", workdir);
        // The DataSource and the ManagedExecutorService of the server as CDI beans, and BeanKindSingleton
        copyCustomSource("Producers.java", sources);
        // Flyway at start, before DoerLifecycle starts Doer
        copyCustomSource("FlywayMigration.java", sources);
        // The data source with system properties: WildFly does not read environment variables there
        copyCustomSource("transitsims-ds.xml", webInf);
        // RESTEasy of WildFly writes JSON with Jackson; Transit Sims uses JSON-B
        copyCustomSource("web.xml", webInf);
        // The pool of the ManagedExecutorService has as many threads as processors (4 on a runner of GitHub Actions)
        // and an unbounded queue: Doer would run fewer tasks at once than its limits. 200, as the pool of Quarkus.
        // The FINE messages of Doer on the console (DEBUG of the console handler): the limits and loaded rows of the
        // sub-queues (ConcurrencyE2E).
        copyCustomSource("server.cli", workdir.resolve("src/main/wildfly"));
        // The WildFly image configured by server.cli, the WAR unpacked, and a start command with exec (standalone.sh
        // turns the exit code 143 into 0)
        copyCustomSource("Dockerfile", workdir);
    }
}
