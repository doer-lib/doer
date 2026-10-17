package com.doer.e2e.runtime;

import java.nio.file.Path;
import java.util.List;

/**
 * Transit Sims in Open Liberty: {@code ROOT.war}, {@code server.xml} and the Postgres driver mounted to {@code /config}. A
 * data source of {@code server.xml} cannot use the driver of the WAR, so the driver is also a file next to it.
 */
class OpenLibertyApp implements E2eApp {
    static final String OPEN_LIBERTY_IMAGE = "icr.io/appcafe/open-liberty:26.0.0.10-full-java25-openj9-ubi-minimal";

    @Override
    public String customSourcesFolder() {
        return "e2e-open-liberty";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path migration = workdir.resolve("src/main/resources/db/migration");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", migration);
        copyCustomSource("pom.xml", workdir.resolve("pom.xml"));
        // The DataSource and the ManagedExecutorService of the server as CDI beans, and BeanKindSingleton
        copyCustomSource("Producers.java", sources.resolve("Producers.java"));
        // Flyway at start, before DoerLifecycle starts Doer
        copyCustomSource("FlywayMigration.java", sources.resolve("FlywayMigration.java"));
        // Flyway finds a classpath location in WebSphere and Open Liberty by this empty file in it
        copyCustomSource("flyway.location", migration.resolve("flyway.location"));
        // Features, the data source, the context root /, INFO on the console, the threads of the ManagedExecutorService
        copyCustomSource("server.xml", workdir.resolve("src/main/liberty/server.xml"));
    }

    /** The command of the image: {@code server run defaultServer}. */
    @Override
    public DockerRunSpec dockerRun(Path workdir) {
        String config = "/opt/ol/wlp/usr/servers/defaultServer/";
        return new DockerRunSpec(List.of(
                "-v", workdir.resolve("src/main/liberty/server.xml") + ":" + config + "server.xml:ro",
                "-v", workdir.resolve("target/liberty/postgresql.jar") + ":" + config + "postgresql.jar:ro",
                "-v", workdir.resolve("target/ROOT.war") + ":" + config + "apps/ROOT.war:ro"),
                OPEN_LIBERTY_IMAGE, List.of());
    }
}
