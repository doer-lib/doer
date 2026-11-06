package transitsims;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/**
 * Flyway at start, before DoerLifecycle starts Doer: on the initialization of the application context, as
 * DoerLifecycle of Helidon. Copied by the e2e suite (HelidonApp).
 */
@ApplicationScoped
public class FlywayMigration {
    @Inject
    DataSource dataSource;

    void migrate(@Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) @Initialized(ApplicationScoped.class)
                 Object event) {
        Flyway.configure().dataSource(dataSource).load().migrate();
    }
}
