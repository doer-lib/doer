package transitsims;

import com.doer.generated._GeneratedDoerService;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.ext.Provider;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.ClassUtils;

/**
 * Transit Sims in Spring Boot: what a Spring application gives Doer. The CDI annotations of Transit Sims are Spring ones
 * here (rewrite.yml), and its JAX-RS resources and providers are Spring beans. Written by the e2e suite (SpringBootApp).
 */
@SpringBootApplication
@Import(_GeneratedDoerService.class) // generated in com.doer.generated, outside the scan
public class SpringBootMain {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootMain.class, args);
    }

    /** The pool of Spring Boot; injected by the qualifier only, so @Inject DataSource gets the proxy below. */
    @Bean(defaultCandidate = false)
    @ConfigurationProperties("spring.datasource.hikari")
    HikariDataSource hikariDataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    /** On a plain DataSource, getConnection() does not join the Spring transaction. */
    @Bean
    DataSource dataSource(@Qualifier("hikariDataSource") HikariDataSource hikari) {
        return new TransactionAwareDataSourceProxy(hikari);
    }

    /** The only Executor (the applicationTaskExecutor of Spring Boot backs off); shut down without waiting. */
    @Bean(destroyMethod = "shutdown")
    ExecutorService executor() {
        return Executors.newCachedThreadPool(new CustomizableThreadFactory("doer-"));
    }

    /**
     * The JAX-RS resources and providers: Spring beans, which Jersey takes from Spring once their classes are registered
     * (Jersey does not scan a Spring Boot jar). A class, not the bean: a proxy has no JAX-RS annotations.
     */
    @Bean
    ResourceConfig resourceConfig(ApplicationContext context) {
        ResourceConfig config = new ResourceConfig();
        config.property(ServerProperties.WADL_FEATURE_DISABLE, true);
        for (var annotation : List.of(Path.class, Provider.class)) {
            for (String name : context.getBeanNamesForAnnotation(annotation)) {
                config.register(ClassUtils.getUserClass(context.getType(name)));
            }
        }
        return config;
    }
}
