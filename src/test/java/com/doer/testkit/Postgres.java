package com.doer.testkit;

import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Postgres in a container, one per JVM, for the SQL tests; it is stopped when the JVM ends. */
public final class Postgres {
    public static final String IMAGE = "postgres:12.3";

    private static PostgreSQLContainer container;
    private static PGSimpleDataSource dataSource;

    private Postgres() {
    }

    /** DataSource of the container; starts it on the first call (or again, when it has stopped). */
    public static synchronized DataSource dataSource() {
        if (dataSource != null && container.isRunning()) {
            return dataSource;
        }
        stop();
        container = new PostgreSQLContainer(IMAGE)
                .withDatabaseName("doer")
                .withUsername("doer")
                .withPassword("doer");
        container.start();
        if (dataSource == null) {
            Runtime.getRuntime().addShutdownHook(new Thread(Postgres::stop));
        }
        dataSource = new PGSimpleDataSource();
        dataSource.setDatabaseName(container.getDatabaseName());
        dataSource.setServerNames(new String[] { container.getHost() });
        dataSource.setPortNumbers(new int[] { container.getMappedPort(5432) });
        dataSource.setUser(container.getUsername());
        dataSource.setPassword(container.getPassword());
        return dataSource;
    }

    private static synchronized void stop() {
        if (container != null) {
            try {
                container.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            container = null;
        }
    }
}
