package com.doer.e2e.runtime;

import static com.doer.testkit.Toolchain.doerLibVersion;

import com.doer.testkit.Processes;
import com.doer.testkit.Sources;
import com.doer.testkit.Toolchain;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Transit Sims in Spring Boot: an executable jar with Jersey, run with {@code java -jar} in a JRE image. The sources become
 * a Spring application: the Spring annotations that have no CDI counterpart are added here, OpenRewrite replaces the CDI
 * annotations with Spring ones ({@code e2e-spring-boot/rewrite.yml}), and {@code e2e-spring-boot} brings the main class
 * and the Spring versions of the classes whose code differs (see src/test/README.md).
 */
class SpringBootApp implements E2eApp {
    static final String REWRITE_PLUGIN_VERSION = "6.46.1";
    static final int GIT_INIT_TIMEOUT_SECONDS = 30;

    /** A class-level JAX-RS annotation, and a CDI scope (a line that starts with it is on the class). */
    static final Pattern JAX_RS_CLASS = Pattern.compile("(?m)^@(Path|Provider)\\b");
    static final Pattern CDI_SCOPE = Pattern.compile("(?m)^@(ApplicationScoped|Dependent|Singleton)\\b");
    /** A line with {@code @Dependent} or {@code @Produces} alone, and its indent. */
    static final Pattern DEPENDENT = Pattern.compile("(?m)^(\\s*)@Dependent$");
    static final Pattern PRODUCES = Pattern.compile("(?m)^(\\s*)@Produces$");
    static final String PROTOTYPE = "$0\n$1@org.springframework.context.annotation.Scope(\"prototype\")";

    @Override
    public String customSourcesFolder() {
        return "e2e-spring-boot";
    }

    @Override
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        Path resources = workdir.resolve("src/main/resources");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", resources.resolve("db/migration"));
        addSpringAnnotations(sources);
        copyCustomSource("pom.xml", workdir.resolve("pom.xml"));
        // The data source and Jersey at /api
        copyCustomSource("application.properties", resources.resolve("application.properties"));
        // The generated service, a transaction-aware DataSource, the Executor and the Jersey resources as Spring beans
        copyCustomSource("SpringBootMain.java", sources.resolve("SpringBootMain.java"));
        // Instead of the one of Transit Sims: a SmartLifecycle, Spring has no Startup and Shutdown events
        copyCustomSource("DoerLifecycle.java", sources.resolve("DoerLifecycle.java"));
        // Instead of the one of Transit Sims: an aspect, Spring has no CDI interceptors
        copyCustomSource("BeanKindTracedInterceptor.java", sources.resolve("validation/BeanKindTracedInterceptor.java"));
        // The recipe of OpenRewrite that replaces the CDI annotations with Spring ones
        copyCustomSource("rewrite.yml", workdir.resolve("rewrite.yml"));
        // OpenRewrite skips the files git ignores, and the work folder is in target/ of this repository
        Processes.run(workdir, "git-init", GIT_INIT_TIMEOUT_SECONDS, List.of("git", "init", "-q")).assertStatus(0);
        Toolchain.mvn(workdir, "mvn-rewrite",
                "org.openrewrite.maven:rewrite-maven-plugin:" + REWRITE_PLUGIN_VERSION + ":runNoFork",
                "-Drewrite.activeRecipes=transitsims.CdiToSpring", "-Ddoer.version=" + doerLibVersion).assertStatus(0);
    }

    @Override
    public DockerRunSpec dockerRun(Path workdir) {
        return new DockerRunSpec(List.of("-v", workdir.resolve("target/transitsims.jar") + ":/app/transitsims.jar:ro"),
                QuarkusApp.JRE_IMAGE, List.of("java", "-jar", "/app/transitsims.jar"));
    }

    /**
     * The Spring annotations that rewrite.yml cannot add (it replaces the CDI ones afterwards): {@code @Scope("prototype")}
     * next to {@code @Dependent} and a CDI {@code @Produces} without a scope, and {@code @Component} on the JAX-RS
     * resources and providers without a scope ({@code SimResource}, ...), so that Jersey takes them from Spring.
     */
    static void addSpringAnnotations(Path sources) throws Exception {
        for (String name : Sources.javaFiles(sources)) {
            Path file = sources.resolve(name);
            String source = DEPENDENT.matcher(Files.readString(file)).replaceAll(PROTOTYPE);
            if (source.contains("import jakarta.enterprise.inject.Produces;")) {
                source = PRODUCES.matcher(source).replaceAll(PROTOTYPE);
            }
            if (JAX_RS_CLASS.matcher(source).find() && !CDI_SCOPE.matcher(source).find()) {
                source = source.replaceFirst("(?m)^public class ", "@org.springframework.stereotype.Component\npublic class ");
            }
            Files.writeString(file, source);
        }
    }
}
