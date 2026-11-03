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
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * A transactional DataSource (Agroal, enlisted in the transactions of Narayana) and an Executor as CDI beans:
 * Helidon MP has neither. The DataSource of Helidon (helidon-integrations-cdi-datasource-hikaricp) is not
 * transactional, and its JTA wrapper (JtaAdaptingDataSource) gives a new connection of the pool to each
 * getConnection() in a transaction, while Doer and Transit Sims take a connection for each statement. Copied by the
 * e2e suite (HelidonApp).
 */
@ApplicationScoped
public class Producers {
    /** Beans of the JTA integration of Helidon (helidon-integrations-cdi-jta-weld). */
    @Inject
    TransactionManager transactionManager;
    @Inject
    TransactionSynchronizationRegistry transactionSynchronizationRegistry;

    /** MicroProfile Config reads them from the environment variables E2E_DB_URL, E2E_DB_USER, E2E_DB_PASSWORD. */
    @Inject
    @ConfigProperty(name = "e2e.db.url")
    String dbUrl;
    @Inject
    @ConfigProperty(name = "e2e.db.user")
    String dbUser;
    @Inject
    @ConfigProperty(name = "e2e.db.password")
    String dbPassword;

    AgroalDataSource dataSource;
    ExecutorService executor;

    @PostConstruct
    void create() {
        try {
            dataSource = AgroalDataSource.from(new AgroalDataSourceConfigurationSupplier()
                    .connectionPoolConfiguration(pool -> pool
                            .maxSize(4)
                            .transactionIntegration(new NarayanaTransactionIntegration(
                                    transactionManager, transactionSynchronizationRegistry))
                            .connectionFactoryConfiguration(factory -> factory
                                    .jdbcUrl(dbUrl)
                                    .principal(new NamePrincipal(dbUser))
                                    .credential(new SimplePassword(dbPassword)))));
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

    @Produces
    DataSource dataSource() {
        return dataSource;
    }

    @Produces
    Executor executor() {
        return executor;
    }
}
