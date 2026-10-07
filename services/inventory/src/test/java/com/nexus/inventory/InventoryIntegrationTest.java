package com.nexus.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.testing.NexusContainers;
import com.nexus.messaging.testing.OutboxConnectors;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Shared stack for Inventory integration tests: Postgres, Kafka and Debezium Connect, started once per
 * test JVM, with the real outbox connector registered. Tests isolate themselves with fresh SKUs and
 * order ids rather than truncating tables.
 */
@SpringBootTest
public abstract class InventoryIntegrationTest {

    private static final Network NETWORK = Network.newNetwork();
    protected static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(NETWORK, "inventory_db");
    protected static final KafkaContainer KAFKA = NexusContainers.kafka(NETWORK);
    protected static final GenericContainer<?> CONNECT = NexusContainers.connect(NETWORK).dependsOn(KAFKA, POSTGRES);

    private static boolean connectorRegistered;

    static {
        Startables.deepStart(POSTGRES, KAFKA, CONNECT).join();
    }

    @DynamicPropertySource
    static void stackProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected ObjectMapper objectMapper;

    @BeforeEach
    void registerConnectorOnce() {
        // Flyway has created the outbox table by the time the Spring context is up.
        synchronized (InventoryIntegrationTest.class) {
            if (!connectorRegistered) {
                OutboxConnectors.register(CONNECT, jdbc, "inventory");
                connectorRegistered = true;
            }
        }
    }

    protected String newSku(int available) {
        String sku = "TEST-" + UUID.randomUUID();
        jdbc.update("INSERT INTO stock (sku, available) VALUES (?, ?)", sku, available);
        return sku;
    }

    protected int available(String sku) {
        return jdbc.queryForObject("SELECT available FROM stock WHERE sku = ?", Integer.class, sku);
    }

    protected int reserved(String sku) {
        return jdbc.queryForObject("SELECT reserved FROM stock WHERE sku = ?", Integer.class, sku);
    }

    protected EventEnvelope reserveCommand(UUID orderId, Map<String, Integer> items) {
        var payload = objectMapper.createObjectNode();
        var array = payload.putArray("items");
        items.forEach((sku, qty) -> array.addObject().put("sku", sku).put("quantity", qty));
        return new EventEnvelope(UUID.randomUUID(), "ReserveInventory", 1, orderId, UUID.randomUUID(), Instant.now(), payload);
    }

    protected EventEnvelope releaseCommand(UUID orderId) {
        var payload = objectMapper.createObjectNode().put("reason", "PAYMENT_DECLINED");
        return new EventEnvelope(UUID.randomUUID(), "ReleaseInventory", 1, orderId, UUID.randomUUID(), Instant.now(), payload);
    }
}
