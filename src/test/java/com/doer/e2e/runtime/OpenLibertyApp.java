package com.doer.e2e.runtime;

import java.nio.file.Path;

/**
 * Transit Sims in Open Liberty: {@code ROOT.war}, unpacked, {@code server.xml} and the Postgres driver in
 * {@code /config} of the image. A data source of {@code server.xml} cannot use the driver of the WAR, so the driver is
 * also a file next to it.
 */
class OpenLibertyApp implements E2eApp {

    @Override
    public String customSourcesFolder() {
        return "e2e-open-liberty";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path migrations = workdir.resolve("src/main/resources/db/migration");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", migrations);
        copyCustomSource("pom.xml", workdir);
        // The DataSource and the ManagedExecutorService of the server as CDI beans, and BeanKindSingleton
        copyCustomSource("Producers.java", sources);
        // Flyway at start, before DoerLifecycle starts Doer
        copyCustomSource("FlywayMigration.java", sources);
        // Flyway finds a classpath location in WebSphere and Open Liberty by this empty file in it
        copyCustomSource("flyway.location", migrations);
        // Features, the data source, the context root /, INFO on the console, the threads of the ManagedExecutorService
        copyCustomSource("server.xml", workdir.resolve("src/main/liberty"));
        // The Open Liberty image with them, prepared by configure.sh of the image: the shared class cache of OpenJ9
        // and the workarea of the server
        copyCustomSource("Dockerfile", workdir);
    }
}
