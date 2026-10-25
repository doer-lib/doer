package transitsims;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/** Flyway at start, before DoerLifecycle starts Doer. Copied by the e2e suite (WildflyApp). */
@ApplicationScoped
public class FlywayMigration {
    @Inject
    DataSource dataSource;

    void migrate(@Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) Startup event) {
        Flyway.configure().dataSource(dataSource).load().migrate();
    }
}
