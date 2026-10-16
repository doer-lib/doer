package com.doer.e2e;

import static com.doer.testkit.Toolchain.doerLibVersion;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Transit Sims as {@code ROOT.war}: what WildFly, Open Liberty and Jetty have in common. The WAR adds the producers of
 * the {@code DataSource} and the {@code Executor} and the Flyway migration, written into the work folder only (see
 * src/test/README.md). A server gives JTA, CDI and JAX-RS; in Jetty the WAR brings them.
 */
abstract class JakartaEeServerApp implements E2eApp {
    static final String JAKARTAEE_API_VERSION = "11.0.0";
    static final String FLYWAY_VERSION = "13.10.0";
    static final String POSTGRESQL_VERSION = "42.7.13";

    @Override
    public void configure(Path workdir) throws Exception {
        copySources("e2e/transitsims", workdir.resolve("src/main/java/transitsims"));
        copySources("e2e/db/migration", workdir.resolve("src/main/resources/db/migration"));
        writeFile(workdir, "pom.xml", warPom(artifactId(), apiDependencies(), extraPlugins()));
        writeFile(workdir, "src/main/java/transitsims/Producers.java", producers());
        writeFile(workdir, "src/main/java/transitsims/FlywayMigration.java", FLYWAY_MIGRATION);
        configureServer(workdir);
    }

    /** {@code artifactId} of the work folder project. */
    abstract String artifactId();

    /** {@code Producers.java}: the {@code DataSource}, the {@code Executor} and what else CDI needs as a bean. */
    abstract String producers();

    /** The Jakarta EE APIs in the {@code pom.xml}: provided by the server. */
    String apiDependencies() {
        return """
                        <dependency>
                            <groupId>jakarta.platform</groupId>
                            <artifactId>jakarta.jakartaee-api</artifactId>
                            <version>%s</version>
                            <scope>provided</scope>
                        </dependency>
                """.formatted(JAKARTAEE_API_VERSION);
    }

    /** Plugins of the {@code pom.xml} besides the compiler and the WAR plugin; empty by default. */
    String extraPlugins() {
        return "";
    }

    /** Writes the configuration of the server. */
    abstract void configureServer(Path workdir) throws IOException;

    static String warPom(String artifactId, String apiDependencies, String extraPlugins) {
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <project xmlns="http://maven.apache.org/POM/4.0.0"
                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
                    <modelVersion>4.0.0</modelVersion>
                    <groupId>transitsims</groupId>
                    <artifactId>%1$s</artifactId>
                    <version>1.0.0-SNAPSHOT</version>
                    <packaging>war</packaging>

                    <properties>
                        <maven.compiler.release>25</maven.compiler.release>
                        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
                        <doer.version>%2$s</doer.version>
                        <flyway.version>%3$s</flyway.version>
                        <postgresql.version>%4$s</postgresql.version>
                    </properties>

                    <dependencies>
                %5$s            <dependency>
                            <groupId>com.java-doer</groupId>
                            <artifactId>doer</artifactId>
                            <version>${doer.version}</version>
                        </dependency>
                        <dependency>
                            <groupId>org.flywaydb</groupId>
                            <artifactId>flyway-core</artifactId>
                            <version>${flyway.version}</version>
                        </dependency>
                        <dependency>
                            <groupId>org.flywaydb</groupId>
                            <artifactId>flyway-database-postgresql</artifactId>
                            <version>${flyway.version}</version>
                        </dependency>
                        <dependency>
                            <groupId>org.postgresql</groupId>
                            <artifactId>postgresql</artifactId>
                            <version>${postgresql.version}</version>
                        </dependency>
                    </dependencies>

                    <build>
                        <finalName>ROOT</finalName>
                        <plugins>
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
                            <plugin>
                                <artifactId>maven-war-plugin</artifactId>
                                <version>3.5.1</version>
                            </plugin>
                %6$s        </plugins>
                    </build>
                </project>
                """.formatted(artifactId, doerLibVersion, FLYWAY_VERSION, POSTGRESQL_VERSION, apiDependencies,
                extraPlugins);
    }

    /** {@code Producers.java} of a server: the data source and the {@code ManagedExecutorService} from JNDI. */
    static String jndiProducers(String dataSourceJndiName) {
        return """
                package transitsims;

                import jakarta.annotation.Resource;
                import jakarta.enterprise.concurrent.ManagedExecutorService;
                import jakarta.enterprise.context.ApplicationScoped;
                import jakarta.enterprise.inject.Produces;
                import jakarta.inject.Singleton;
                import java.util.concurrent.Executor;
                import javax.sql.DataSource;
                import transitsims.validation.BeanKindSingleton;
                import transitsims.validation.CallTrace;

                /** The DataSource and the Executor of the server as CDI beans. Written by the e2e suite (JakartaEeServerApp). */
                @ApplicationScoped
                public class Producers {
                    @Resource(lookup = "%s")
                    DataSource dataSource;

                    /**
                     * The threads of Doer get the context of the application, and each task its own local transaction
                     * containment: a connection taken outside a transaction goes back to the pool when the task ends (Open
                     * Liberty). Not a pool of the ManagedThreadFactory: the containment of its threads never ends.
                     */
                    @Resource
                    ManagedExecutorService executor;

                    @Produces
                    DataSource dataSource() {
                        return dataSource;
                    }

                    @Produces
                    Executor executor() {
                        return executor;
                    }
                %s}
                """.formatted(dataSourceJndiName, BEAN_KIND_SINGLETON_PRODUCER);
    }

    /** A member of {@code Producers}: the bean of a class that CDI does not discover. */
    static final String BEAN_KIND_SINGLETON_PRODUCER = """

                /**
                 * @Singleton alone does not make a class a CDI bean: it is not a bean defining annotation, so a WAR
                 * without beans.xml (bean discovery mode annotated) does not discover BeanKindSingleton.
                 */
                @Produces
                @Singleton
                BeanKindSingleton beanKindSingleton(CallTrace callTrace) {
                    BeanKindSingleton bean = new BeanKindSingleton();
                    bean.setCallTrace(callTrace);
                    return bean;
                }
            """;

    /** Migrates the database before Doer starts: CDI calls this observer of Startup before the one of DoerLifecycle. */
    static final String FLYWAY_MIGRATION = """
            package transitsims;

            import jakarta.annotation.Priority;
            import jakarta.enterprise.context.ApplicationScoped;
            import jakarta.enterprise.event.Observes;
            import jakarta.enterprise.event.Startup;
            import jakarta.inject.Inject;
            import jakarta.interceptor.Interceptor;
            import javax.sql.DataSource;
            import org.flywaydb.core.Flyway;

            /** Flyway at start, before DoerLifecycle starts Doer. Written by the e2e suite (JakartaEeServerApp). */
            @ApplicationScoped
            public class FlywayMigration {
                @Inject
                DataSource dataSource;

                void migrate(@Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) Startup event) {
                    Flyway.configure().dataSource(dataSource).load().migrate();
                }
            }
            """;
}
