package com.doer.e2e.runtime;

import static com.doer.testkit.Toolchain.doerLibVersion;

import com.doer.testkit.Processes;
import com.doer.testkit.Toolchain;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Transit Sims in Spring Boot: an executable jar with Jersey, run with {@code java -jar} in a JRE image. The sources become
 * a Spring application: the Spring annotations that have no CDI counterpart are added here, OpenRewrite replaces the CDI
 * annotations with Spring ones ({@code e2e-spring-boot/rewrite.yml}), and {@code e2e-spring-boot} brings the main class
 * and the Spring versions of the classes whose code differs (see src/test/README.md).
 */
class SpringBootApp implements E2eApp {
    static final String REWRITE_PLUGIN_VERSION = "6.46.1";
    static final int GIT_INIT_TIMEOUT_SECONDS = 30;

    static final String PROTOTYPE = "@org.springframework.context.annotation.Scope(\"prototype\")";
    static final String COMPONENT = "@org.springframework.stereotype.Component";

    @Override
    public String customSourcesFolder() {
        return "e2e-spring-boot";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path migrations = workdir.resolve("src/main/resources/db/migration");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", migrations);
        addSpringAnnotations(sources);
        copyCustomSource("pom.xml", workdir);
        // The data source and Jersey at /api
        copyCustomSource("application.properties", workdir.resolve("src/main/resources"));
        // The generated service, a transaction-aware DataSource, the Executor and the Jersey resources as Spring beans
        copyCustomSource("SpringBootMain.java", sources);
        // Instead of the one of Transit Sims: a SmartLifecycle, Spring has no Startup and Shutdown events
        copyCustomSource("DoerLifecycle.java", sources);
        // Instead of the one of Transit Sims: an aspect, Spring has no CDI interceptors
        copyCustomSource("BeanKindTracedInterceptor.java", sources.resolve("validation"));
        // The recipe of OpenRewrite that replaces the CDI annotations with Spring ones
        copyCustomSource("rewrite.yml", workdir);
        // OpenRewrite skips the files git ignores, and the work folder is in target/ of this repository
        Processes.run(workdir, "git-init", GIT_INIT_TIMEOUT_SECONDS, List.of("git", "init", "-q")).assertStatus(0);
        Toolchain.mvn(workdir, "mvn-rewrite",
                "org.openrewrite.maven:rewrite-maven-plugin:" + REWRITE_PLUGIN_VERSION + ":runNoFork",
                "-Drewrite.activeRecipes=transitsims.CdiToSpring", "-Ddoer.version=" + doerLibVersion).assertStatus(0);
    }

    @Override
    public DockerRunSpec dockerRun(Path workdir) {
        return new DockerRunSpec(List.of("-v", workdir.resolve("target/transitsims.jar") + ":/app/transitsims.jar:ro"),
                JRE_IMAGE, List.of("java", "-jar", "/app/transitsims.jar"));
    }

    /**
     * The Spring annotations that rewrite.yml cannot add (it replaces the CDI ones afterwards). A new bean of these kinds
     * in Transit Sims needs a line here.
     */
    static void addSpringAnnotations(Path sources) throws IOException {
        // @Dependent: one instance per injection point
        insertBefore(sources.resolve("SimSupervisor.java"), "public class SimSupervisor", PROTOTYPE + "\n");
        insertBefore(sources.resolve("validation/ConcurrencyQueues.java"), "public class ConcurrencyQueues",
                PROTOTYPE + "\n");
        // A CDI producer without a scope makes a @Dependent bean
        insertBefore(sources.resolve("validation/BeanKindProducers.java"), "public BeanKindProduced produce()",
                PROTOTYPE + " ");
        // JAX-RS resources and providers without a scope: Spring beans, so that Jersey takes them from Spring
        insertBefore(sources.resolve("SimResource.java"), "public class SimResource", COMPONENT + "\n");
        insertBefore(sources.resolve("BusResource.java"), "public class BusResource", COMPONENT + "\n");
        insertBefore(sources.resolve("RestExceptionMapper.java"), "public class RestExceptionMapper", COMPONENT + "\n");
    }

    /** Inserts the annotation before the text; fails if the file has no such text. */
    static void insertBefore(Path file, String text, String annotation) throws IOException {
        String source = Files.readString(file);
        if (!source.contains(text)) {
            throw new IllegalStateException("No '" + text + "' in " + file);
        }
        Files.writeString(file, source.replace(text, annotation + text));
    }
}
