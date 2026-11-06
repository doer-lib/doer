package com.doer.e2e.runtime;

import java.nio.file.Path;

/**
 * Transit Sims in Helidon MP: a jar with its dependencies in {@code libs}, run with {@code java -jar} in a JRE image.
 * Helidon brings CDI (Weld SE), JAX-RS (Jersey) and JTA (Narayana); the application brings a transactional
 * {@code DataSource} (Agroal with its Narayana integration).
 */
class HelidonApp implements E2eApp {

    @Override
    public String customSourcesFolder() {
        return "e2e-helidon";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path resources = workdir.resolve("src/main/resources");
        Path metaInf = resources.resolve("META-INF");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", resources.resolve("db/migration"));
        copyCustomSource("pom.xml", workdir);
        // A transactional DataSource of Agroal and Narayana and an Executor as CDI beans: Helidon has neither
        copyCustomSource("Producers.java", sources);
        // Instead of the one of Transit Sims: on Startup Helidon already accepts requests, so Doer starts on the
        // initialization of the application context, before the web server
        copyCustomSource("DoerLifecycle.java", sources);
        // Flyway at start, before DoerLifecycle starts Doer
        copyCustomSource("FlywayMigration.java", sources);
        // Weld SE discovers the beans of the jar only with beans.xml. Mode all: Jersey makes the JAX-RS resources
        // without a scope (SimResource, BusResource) CDI beans only if CDI discovers them, and the @Singleton class
        // BeanKindSingleton is a bean too
        copyCustomSource("beans.xml", metaInf);
        // The port, and the database of Producers
        copyCustomSource("microprofile-config.properties", metaInf);
        // The format of java.util.logging on stdout, and the FINE messages of Doer
        copyCustomSource("logging.properties", resources);
        // The jar and libs in a JRE image
        copyCustomSource("Dockerfile", workdir);
    }
}
