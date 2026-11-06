package com.doer.e2e.runtime;

import java.nio.file.Path;

/**
 * Transit Sims in Jetty, a servlet container: {@code ROOT.war} brings CDI (Weld servlet), JAX-RS (Jersey), JTA
 * (Narayana and its CDI interceptors of {@code @Transactional}) and a transactional {@code DataSource} (Agroal with its
 * Narayana integration).
 */
class JettyApp implements E2eApp {

    @Override
    public String customSourcesFolder() {
        return "e2e-jetty";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path migrations = workdir.resolve("src/main/resources/db/migration");
        Path webInf = workdir.resolve("src/main/webapp/WEB-INF");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", migrations);
        copyCustomSource("pom.xml", workdir);
        // A transactional DataSource of Agroal and Narayana and an Executor as CDI beans: Jetty has neither
        copyCustomSource("Producers.java", sources);
        // Flyway at start, before DoerLifecycle starts Doer
        copyCustomSource("FlywayMigration.java", sources);
        // Weld servlet discovers the beans of WEB-INF/classes only with beans.xml. Mode all: the extension of Jersey
        // (jersey-cdi1x) makes the JAX-RS resources without a scope (SimResource, BusResource) CDI beans only if CDI
        // discovers them; Jersey creates such a @Dependent resource per request. So the @Singleton class
        // BeanKindSingleton is a bean too, and needs no producer here.
        copyCustomSource("beans.xml", webInf);
        // The listener of Weld and the servlet of Jersey at /api
        copyCustomSource("web.xml", webInf);
        // The format of java.util.logging, and the FINE messages of Doer
        copyCustomSource("logging.properties", workdir.resolve("src/main/jetty"));
        // The WAR, unpacked, in the Jetty image with the modules of Jakarta EE 11 and the command line of the server
        // (jetty.start)
        copyCustomSource("Dockerfile", workdir);
    }
}
