package com.doer.e2e.runtime;

import java.nio.file.Path;

/** Transit Sims in Quarkus: a Quarkus JVM application, run with {@code java -jar quarkus-run.jar} in a JRE image. */
class QuarkusApp implements E2eApp {

    @Override
    public String customSourcesFolder() {
        return "e2e-quarkus";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path migrations = workdir.resolve("src/main/resources/db/migration");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", migrations);
        copyCustomSource("pom.xml", workdir);
        // The data source and Flyway at start
        copyCustomSource("application.properties", workdir.resolve("src/main/resources"));
        // quarkus-app in a JRE image
        copyCustomSource("Dockerfile", workdir);
    }
}
