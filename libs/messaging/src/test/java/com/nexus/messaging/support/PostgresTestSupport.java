package com.nexus.messaging.support;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import com.nexus.messaging.testing.NexusContainers;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One Postgres container per test JVM, migrated with the library's own Flyway scripts.
 */
public abstract class PostgresTestSupport {

    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(NexusContainers.POSTGRES_IMAGE);

    protected static final DriverManagerDataSource DATA_SOURCE;
    protected static final JdbcTemplate JDBC;
    protected static final TransactionTemplate TX;

    static {
        POSTGRES.start();
        DATA_SOURCE = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Flyway.configure().dataSource(DATA_SOURCE).locations("classpath:db/migration").load().migrate();
        JDBC = new JdbcTemplate(DATA_SOURCE);
        TX = new TransactionTemplate(new DataSourceTransactionManager(DATA_SOURCE));
    }

    @BeforeEach
    void cleanMessagingTables() {
        JDBC.execute("TRUNCATE outbox, processed_events");
    }
}
