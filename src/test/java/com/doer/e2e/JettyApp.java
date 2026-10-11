package com.doer.e2e;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Transit Sims in Jetty, a servlet container: {@code ROOT.war} brings CDI (Weld servlet), JAX-RS (Jersey), JTA
 * (Narayana and its CDI interceptors of {@code @Transactional}) and a transactional {@code DataSource} (Agroal with its
 * Narayana integration).
 */
class JettyApp extends JakartaEeServerApp {
    static final String JETTY_IMAGE = "jetty:12.1.14-jdk25";
    static final String WELD_VERSION = "6.0.4.Final";
    static final String JERSEY_VERSION = "4.0.3";
    static final String NARAYANA_VERSION = "7.3.4.Final";
    static final String AGROAL_VERSION = "3.2.1";

    @Override
    String artifactId() {
        return "transitsims-jetty";
    }

    /** No {@code jakartaee-api}: the APIs come with their implementations into {@code WEB-INF/lib}. */
    @Override
    String apiDependencies() {
        return """
                        <dependency>
                            <groupId>org.jboss.weld.servlet</groupId>
                            <artifactId>weld-servlet-core</artifactId>
                            <version>%1$s</version>
                        </dependency>
                        <dependency>
                            <groupId>org.glassfish.jersey.containers</groupId>
                            <artifactId>jersey-container-servlet</artifactId>
                            <version>%2$s</version>
                        </dependency>
                        <dependency>
                            <groupId>org.glassfish.jersey.inject</groupId>
                            <artifactId>jersey-hk2</artifactId>
                            <version>%2$s</version>
                        </dependency>
                        <dependency>
                            <groupId>org.glassfish.jersey.ext.cdi</groupId>
                            <artifactId>jersey-cdi1x-servlet</artifactId>
                            <version>%2$s</version>
                        </dependency>
                        <dependency>
                            <groupId>org.glassfish.jersey.media</groupId>
                            <artifactId>jersey-media-json-binding</artifactId>
                            <version>%2$s</version>
                        </dependency>
                        <dependency>
                            <groupId>org.glassfish.jersey.media</groupId>
                            <artifactId>jersey-media-json-processing</artifactId>
                            <version>%2$s</version>
                        </dependency>
                        <dependency>
                            <groupId>org.jboss.narayana.jta</groupId>
                            <artifactId>narayana-jta</artifactId>
                            <version>%3$s</version>
                        </dependency>
                        <dependency>
                            <groupId>org.jboss.narayana.jta</groupId>
                            <artifactId>cdi</artifactId>
                            <version>%3$s</version>
                        </dependency>
                        <dependency>
                            <groupId>jakarta.transaction</groupId>
                            <artifactId>jakarta.transaction-api</artifactId>
                            <version>2.0.1</version>
                        </dependency>
                        <dependency>
                            <groupId>io.agroal</groupId>
                            <artifactId>agroal-pool</artifactId>
                            <version>%4$s</version>
                        </dependency>
                        <dependency>
                            <groupId>io.agroal</groupId>
                            <artifactId>agroal-narayana</artifactId>
                            <version>%4$s</version>
                        </dependency>
                """.formatted(WELD_VERSION, JERSEY_VERSION, NARAYANA_VERSION, AGROAL_VERSION);
    }

    @Override
    String producers() {
        return """
                package transitsims;

                import io.agroal.api.AgroalDataSource;
                import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
                import io.agroal.api.security.NamePrincipal;
                import io.agroal.api.security.SimplePassword;
                import io.agroal.narayana.NarayanaTransactionIntegration;
                import jakarta.annotation.PostConstruct;
                import jakarta.annotation.PreDestroy;
                import jakarta.enterprise.context.ApplicationScoped;
                import jakarta.enterprise.inject.Produces;
                import jakarta.inject.Inject;
                import jakarta.transaction.TransactionManager;
                import jakarta.transaction.TransactionSynchronizationRegistry;
                import java.sql.SQLException;
                import java.util.concurrent.Executor;
                import java.util.concurrent.ExecutorService;
                import java.util.concurrent.Executors;
                import javax.sql.DataSource;

                /**
                 * A transactional DataSource (Agroal, enlisted in the transactions of Narayana) and an Executor as CDI beans:
                 * Jetty has neither. Written by the e2e suite (JettyApp).
                 */
                @ApplicationScoped
                public class Producers {
                    /** Beans of the CDI extension of Narayana. */
                    @Inject
                    TransactionManager transactionManager;
                    @Inject
                    TransactionSynchronizationRegistry transactionSynchronizationRegistry;

                    AgroalDataSource dataSource;
                    ExecutorService executor;

                    @PostConstruct
                    void create() {
                        // The defaults are for a run from the IDE (doer.e2e.runtime=external)
                        try {
                            dataSource = AgroalDataSource.from(new AgroalDataSourceConfigurationSupplier()
                                    .connectionPoolConfiguration(pool -> pool
                                            .maxSize(4)
                                            .transactionIntegration(new NarayanaTransactionIntegration(
                                                    transactionManager, transactionSynchronizationRegistry))
                                            .connectionFactoryConfiguration(factory -> factory
                                                    .jdbcUrl(env("E2E_DB_URL", "jdbc:postgresql://localhost:5432/doer"))
                                                    .principal(new NamePrincipal(env("E2E_DB_USER", "doer")))
                                                    .credential(new SimplePassword(env("E2E_DB_PASSWORD", "doer"))))));
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                        executor = Executors.newCachedThreadPool();
                    }

                    @PreDestroy
                    void close() {
                        executor.shutdown();
                        dataSource.close();
                    }

                    static String env(String name, String defaultValue) {
                        String value = System.getenv(name);
                        return value == null ? defaultValue : value;
                    }

                    @Produces
                    DataSource dataSource() {
                        return dataSource;
                    }

                    @Produces
                    Executor executor() {
                        return executor;
                    }
                }
                """;
    }

    @Override
    void configureServer(Path workdir) throws IOException {
        // Weld servlet discovers the beans of WEB-INF/classes only with beans.xml. Mode all: the extension of Jersey
        // (jersey-cdi1x) makes the JAX-RS resources without a scope (SimResource, BusResource) CDI beans only if CDI
        // discovers them; Jersey creates such a @Dependent resource per request. So the @Singleton class
        // BeanKindSingleton is a bean too, and needs no producer here.
        writeFile(workdir, "src/main/webapp/WEB-INF/beans.xml", """
                <?xml version="1.0" encoding="UTF-8"?>
                <beans xmlns="https://jakarta.ee/xml/ns/jakartaee" version="4.1" bean-discovery-mode="all"/>
                """);
        writeFile(workdir, "src/main/webapp/WEB-INF/web.xml", """
                <?xml version="1.0" encoding="UTF-8"?>
                <web-app xmlns="https://jakarta.ee/xml/ns/jakartaee" version="6.1">
                    <listener>
                        <listener-class>org.jboss.weld.environment.servlet.Listener</listener-class>
                    </listener>
                    <servlet>
                        <servlet-name>jersey</servlet-name>
                        <servlet-class>org.glassfish.jersey.servlet.ServletContainer</servlet-class>
                        <init-param>
                            <param-name>jersey.config.server.provider.packages</param-name>
                            <param-value>transitsims</param-value>
                        </init-param>
                        <load-on-startup>1</load-on-startup>
                    </servlet>
                    <servlet-mapping>
                        <servlet-name>jersey</servlet-name>
                        <url-pattern>/api/*</url-pattern>
                    </servlet-mapping>
                </web-app>
                """);
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
    public DockerRun dockerRun(Path workdir) {
        return new DockerRun(List.of("-v", workdir.resolve("target/ROOT.war") + ":/var/lib/jetty/webapps/ROOT.war:ro"),
                JETTY_IMAGE, List.of("sh", "-c", START));
    }
}
