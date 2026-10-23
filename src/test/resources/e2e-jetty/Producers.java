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
 * Jetty has neither. Copied by the e2e suite (JettyApp).
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
