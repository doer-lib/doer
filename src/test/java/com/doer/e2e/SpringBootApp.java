package com.doer.e2e;

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
 * annotations with Spring ones ({@code e2e/spring-boot/rewrite.yml}), and {@code e2e/spring-boot} brings the main class
 * and the Spring versions of the classes whose code differs (see src/test/README.md).
 */
class SpringBootApp implements E2eApp {
    static final String SPRING_BOOT_VERSION = "4.1.1";
    static final String CDI_API_VERSION = "4.1.0";
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
    public void configure(Path workdir) throws Exception {
        Path sources = workdir.resolve("src/main/java/transitsims");
        copySources("e2e/transitsims", sources);
        copySources("e2e/db/migration", workdir.resolve("src/main/resources/db/migration"));
        addSpringAnnotations(sources);
        copySources("e2e/spring-boot", workdir);
        writeFile(workdir, "pom.xml", POM.formatted(SPRING_BOOT_VERSION, doerLibVersion, CDI_API_VERSION));
        writeFile(workdir, "src/main/resources/application.properties", APPLICATION_PROPERTIES);
        // OpenRewrite skips the files git ignores, and the work folder is in target/ of this repository
        Processes.run(workdir, "git-init", GIT_INIT_TIMEOUT_SECONDS, List.of("git", "init", "-q")).assertStatus(0);
        Toolchain.mvn(workdir, "mvn-rewrite",
                "org.openrewrite.maven:rewrite-maven-plugin:" + REWRITE_PLUGIN_VERSION + ":runNoFork",
                "-Drewrite.activeRecipes=transitsims.CdiToSpring").assertStatus(0);
    }

    @Override
    public DockerRun dockerRun(Path workdir) {
        return new DockerRun(List.of("-v", workdir.resolve("target/transitsims.jar") + ":/app/transitsims.jar:ro"),
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

    static final String POM = """
            <?xml version="1.0" encoding="UTF-8"?>
            <project xmlns="http://maven.apache.org/POM/4.0.0"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
                <modelVersion>4.0.0</modelVersion>
                <parent>
                    <groupId>org.springframework.boot</groupId>
                    <artifactId>spring-boot-starter-parent</artifactId>
                    <version>%1$s</version>
                    <relativePath/>
                </parent>
                <groupId>transitsims</groupId>
                <artifactId>transitsims-spring-boot</artifactId>
                <version>1.0.0-SNAPSHOT</version>

                <properties>
                    <java.version>25</java.version>
                    <doer.version>%2$s</doer.version>
                </properties>

                <dependencies>
                    <dependency>
                        <groupId>org.springframework.boot</groupId>
                        <artifactId>spring-boot-starter-jersey</artifactId>
                        <exclusions>
                            <!-- Transit Sims uses JSON-B annotations -->
                            <exclusion>
                                <groupId>org.glassfish.jersey.media</groupId>
                                <artifactId>jersey-media-json-jackson</artifactId>
                            </exclusion>
                        </exclusions>
                    </dependency>
                    <dependency>
                        <groupId>org.glassfish.jersey.media</groupId>
                        <artifactId>jersey-media-json-binding</artifactId>
                    </dependency>
                    <dependency>
                        <groupId>org.glassfish.jersey.media</groupId>
                        <artifactId>jersey-media-json-processing</artifactId>
                    </dependency>
                    <dependency>
                        <groupId>org.springframework.boot</groupId>
                        <artifactId>spring-boot-starter-jdbc</artifactId>
                    </dependency>
                    <dependency>
                        <groupId>org.springframework.boot</groupId>
                        <artifactId>spring-boot-starter-flyway</artifactId>
                    </dependency>
                    <dependency>
                        <!-- The aspect of BeanKindTracedInterceptor -->
                        <groupId>org.springframework.boot</groupId>
                        <artifactId>spring-boot-starter-aspectj</artifactId>
                    </dependency>
                    <dependency>
                        <groupId>org.flywaydb</groupId>
                        <artifactId>flyway-database-postgresql</artifactId>
                    </dependency>
                    <dependency>
                        <groupId>org.postgresql</groupId>
                        <artifactId>postgresql</artifactId>
                        <scope>runtime</scope>
                    </dependency>
                    <dependency>
                        <!-- The generated service is @ApplicationScoped; also the types rewrite.yml replaces -->
                        <groupId>jakarta.enterprise</groupId>
                        <artifactId>jakarta.enterprise.cdi-api</artifactId>
                        <version>%3$s</version>
                    </dependency>
                    <dependency>
                        <!-- @jakarta.transaction.Transactional, which Spring supports when this API is present -->
                        <groupId>jakarta.transaction</groupId>
                        <artifactId>jakarta.transaction-api</artifactId>
                    </dependency>
                    <dependency>
                        <groupId>com.java-doer</groupId>
                        <artifactId>doer</artifactId>
                        <version>${doer.version}</version>
                    </dependency>
                </dependencies>

                <build>
                    <finalName>transitsims</finalName>
                    <plugins>
                        <plugin>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-maven-plugin</artifactId>
                        </plugin>
                        <plugin>
                            <artifactId>maven-compiler-plugin</artifactId>
                            <configuration>
                                <parameters>true</parameters>
                                <annotationProcessorPaths>
                                    <path>
                                        <groupId>com.java-doer</groupId>
                                        <artifactId>doer</artifactId>
                                        <version>${doer.version}</version>
                                    </path>
                                </annotationProcessorPaths>
                            </configuration>
                        </plugin>
                    </plugins>
                </build>
            </project>
            """;

    static final String APPLICATION_PROPERTIES = """
            # The defaults are for a run from the IDE (doer.e2e.runtime=external)
            spring.datasource.url=${E2E_DB_URL:jdbc:postgresql://localhost:5432/doer}
            spring.datasource.username=${E2E_DB_USER:doer}
            spring.datasource.password=${E2E_DB_PASSWORD:doer}
            spring.datasource.hikari.maximum-pool-size=4
            # @ApplicationPath of TransitSimsApplication: Spring Boot reads the path from the ResourceConfig bean
            spring.jersey.application-path=/api
            server.port=8080
            logging.pattern.console=%d{yyyy-MM-dd HH:mm:ss.SSS} %p {%t} %logger{1} - %m%n
            """;
}
