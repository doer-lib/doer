package com.doer.e2e.runtime;

import java.nio.file.Path;
import java.util.List;

/**
 * Transit Sims in Jetty, a servlet container: {@code ROOT.war} brings CDI (Weld servlet), JAX-RS (Jersey), JTA
 * (Narayana and its CDI interceptors of {@code @Transactional}) and a transactional {@code DataSource} (Agroal with its
 * Narayana integration).
 */
class JettyApp implements E2eApp {
    static final String JETTY_IMAGE = "jetty:12.1.14-jdk25";

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
    }

    /**
     * {@code start.jar} with the modules of Jakarta EE 11; the entrypoint of the image runs it with {@code exec}. Jetty
     * and {@code java.util.logging} write to stderr, and the tests read stdout, so stderr goes to stdout.
     */
    static final String START = """
            exec /docker-entrypoint.sh java -Djava.util.logging.SimpleFormatter.format='%1$tF %1$tT.%1$tL %4$s {%3$s} %5$s%6$s%n' \\
                -jar "$JETTY_HOME/start.jar" --module=ee11-deploy,ee11-cdi-decorate 2>&1
            """;

    @Override
    public DockerRunSpec dockerRun(Path workdir) {
        return new DockerRunSpec(
                List.of("-v", workdir.resolve("target/ROOT.war") + ":/var/lib/jetty/webapps/ROOT.war:ro"),
                JETTY_IMAGE, List.of("sh", "-c", START));
    }
}
