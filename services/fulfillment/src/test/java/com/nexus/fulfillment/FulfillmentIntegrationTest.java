package com.nexus.fulfillment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.messaging.envelope.EnvelopeMapper;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Shared stack for Fulfillment integration tests (see {@code InventoryIntegrationTest} for the pattern).
 */
@SpringBootTest
public abstract class FulfillmentIntegrationTest {

    protected static final String RESTRICTED_SKU = "SKU-HAZMAT-01";

    private static final Network NETWORK = Network.newNetwork();
    protected static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(NETWORK, "fulfillment_db");
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

    @Autowired
    protected EnvelopeMapper envelopeMapper;

    @BeforeEach
    void registerConnectorOnce() {
        synchronized (FulfillmentIntegrationTest.class) {
            if (!connectorRegistered) {
                OutboxConnectors.register(CONNECT, jdbc, "fulfillment");
                connectorRegistered = true;
            }
        }
    }

    protected EventEnvelope createCommand(UUID orderId, String customerId, Map<String, Integer> items) {
        var payload = objectMapper.createObjectNode().put("customerId", customerId);
        var array = payload.putArray("items");
        items.forEach((sku, qty) -> array.addObject().put("sku", sku).put("quantity", qty));
        return message("CreateShipment", orderId, payload);
    }

    protected EventEnvelope createCommand(UUID orderId) {
        return createCommand(orderId, "c-" + orderId, Map.of("SKU-0001", 2));
    }

    protected EventEnvelope cancelCommand(UUID orderId) {
        return message("CancelShipment", orderId, objectMapper.createObjectNode().put("reason", "TIMEOUT_CREATE_SHIPMENT"));
    }

    protected EventEnvelope orderCompleted(UUID orderId, String customerId) {
        return message("OrderCompleted", orderId, objectMapper.createObjectNode()
                .put("totalCents", 3998).put("currency", "USD").put("customerId", customerId));
    }

    protected EventEnvelope orderCancelled(UUID orderId, String reason) {
        return message("OrderCancelled", orderId, objectMapper.createObjectNode().put("reason", reason));
    }

    protected EventEnvelope message(String type, UUID orderId, JsonNode payload) {
        return new EventEnvelope(UUID.randomUUID(), type, 1, orderId, UUID.randomUUID(), Instant.now(), payload);
    }

    /** Events written to the outbox for the order, oldest first. */
    protected List<EventEnvelope> replies(UUID orderId) {
        return jdbc.query("SELECT payload::text FROM outbox WHERE aggregate_id = ? ORDER BY created_at",
                (rs, i) -> envelopeMapper.fromJson(rs.getString(1)), orderId.toString());
    }

    protected String shipmentStatus(UUID orderId) {
        return jdbc.query("SELECT status FROM shipments WHERE order_id = ?", (rs, i) -> rs.getString(1), orderId)
                .stream().findFirst().orElse(null);
    }

    protected List<String> notifications(UUID orderId) {
        return jdbc.queryForList("SELECT type FROM notifications WHERE order_id = ? ORDER BY created_at, type",
                String.class, orderId);
    }
}
