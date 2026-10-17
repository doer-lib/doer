package com.doer.e2e.runtime;

import java.nio.file.Path;
import java.util.List;

/** Transit Sims in Quarkus: a Quarkus JVM application, run with {@code java -jar quarkus-run.jar} in a JRE image. */
class QuarkusApp implements E2eApp {
    static final String JRE_IMAGE = "eclipse-temurin:25-jre";

    @Override
    public String customSourcesFolder() {
        return "e2e-quarkus";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path resources = workdir.resolve("src/main/resources");
        copySources("e2e/transitsims", workdir.resolve("src/main/java/transitsims"));
        copySources("e2e/db/migration", resources.resolve("db/migration"));
        copyCustomSource("pom.xml", workdir.resolve("pom.xml"));
        // The data source and Flyway at start
        copyCustomSource("application.properties", resources.resolve("application.properties"));
    }

    @Override
    public DockerRunSpec dockerRun(Path workdir) {
        return new DockerRunSpec(List.of("-v", workdir.resolve("target/quarkus-app") + ":/app:ro"),
                JRE_IMAGE, List.of("java", "-jar", "/app/quarkus-run.jar"));
    }
}
