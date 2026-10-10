package com.doer.e2e;

import static com.doer.testkit.Toolchain.doerLibVersion;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** CarWash in Quarkus: a Quarkus JVM application, run with {@code java -jar quarkus-run.jar} in a JRE image. */
class QuarkusApp implements E2eApp {
    static final String QUARKUS_VERSION = "3.40.1";
    static final String JRE_IMAGE = "eclipse-temurin:25-jre";

    @Override
    public void configure(Path workdir) throws Exception {
        copySources("e2e/carwash", workdir.resolve("src/main/java/carwash"));
        copySources("e2e/db/migration", workdir.resolve("src/main/resources/db/migration"));
        configureQuarkusApp(workdir);
    }

    @Override
    public DockerRun dockerRun(Path workdir) {
        return quarkusDockerRun(workdir);
    }

    void configureQuarkusApp(Path workdir) throws IOException {
        writeFile(workdir, "pom.xml", """
                <?xml version="1.0" encoding="UTF-8"?>
                <project xmlns="http://maven.apache.org/POM/4.0.0"
                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
                    <modelVersion>4.0.0</modelVersion>
                    <groupId>carwash</groupId>
                    <artifactId>carwash-quarkus</artifactId>
                    <version>1.0.0-SNAPSHOT</version>
                    <packaging>quarkus</packaging>

                    <properties>
                        <maven.compiler.release>25</maven.compiler.release>
                        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
                        <quarkus.version>%1$s</quarkus.version>
                        <doer.version>%2$s</doer.version>
                    </properties>

                    <dependencyManagement>
                        <dependencies>
                            <dependency>
                                <groupId>io.quarkus.platform</groupId>
                                <artifactId>quarkus-bom</artifactId>
                                <version>${quarkus.version}</version>
                                <type>pom</type>
                                <scope>import</scope>
                            </dependency>
                        </dependencies>
                    </dependencyManagement>

                    <dependencies>
                        <dependency>
                            <groupId>io.quarkus</groupId>
                            <artifactId>quarkus-resteasy</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>io.quarkus</groupId>
                            <artifactId>quarkus-resteasy-jsonb</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>io.quarkus</groupId>
                            <artifactId>quarkus-jdbc-postgresql</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>io.quarkus</groupId>
                            <artifactId>quarkus-flyway</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.flywaydb</groupId>
                            <artifactId>flyway-database-postgresql</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>com.java-doer</groupId>
                            <artifactId>doer</artifactId>
                            <version>${doer.version}</version>
                        </dependency>
                    </dependencies>

                    <build>
                        <plugins>
                            <plugin>
                                <groupId>io.quarkus.platform</groupId>
                                <artifactId>quarkus-maven-plugin</artifactId>
                                <version>${quarkus.version}</version>
                                <extensions>true</extensions>
                            </plugin>
                            <plugin>
                                <artifactId>maven-compiler-plugin</artifactId>
                                <version>3.15.0</version>
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
                """.formatted(QUARKUS_VERSION, doerLibVersion));
        writeFile(workdir, "src/main/resources/application.properties", """
                quarkus.datasource.db-kind=postgresql
                # The defaults are for a run from the IDE (doer.e2e.runtime=external)
                quarkus.datasource.jdbc.url=${E2E_DB_URL:jdbc:postgresql://localhost:5432/doer}
                quarkus.datasource.username=${E2E_DB_USER:doer}
                quarkus.datasource.password=${E2E_DB_PASSWORD:doer}
                quarkus.datasource.jdbc.max-size=4
                quarkus.flyway.migrate-at-start=true
                quarkus.http.port=8080
                quarkus.log.console.format=%d{yyyy-MM-dd HH:mm:ss,SSS} %p {%t} %c{1.} - %s%e%n
                """);
    }

    DockerRun quarkusDockerRun(Path workdir) {
        return new DockerRun(List.of("-v", workdir.resolve("target/quarkus-app") + ":/app:ro"),
                JRE_IMAGE, List.of("java", "-jar", "/app/quarkus-run.jar"));
    }
}
