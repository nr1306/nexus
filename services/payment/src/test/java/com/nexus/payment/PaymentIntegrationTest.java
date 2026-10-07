package com.nexus.payment;

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
import java.util.UUID;

/**
 * Shared stack for Payment integration tests (see {@code InventoryIntegrationTest} for the pattern).
 */
@SpringBootTest
public abstract class PaymentIntegrationTest {

    private static final Network NETWORK = Network.newNetwork();
    protected static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(NETWORK, "payment_db");
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
        synchronized (PaymentIntegrationTest.class) {
            if (!connectorRegistered) {
                OutboxConnectors.register(CONNECT, jdbc, "payment");
                connectorRegistered = true;
            }
        }
    }

    protected EventEnvelope authorizeCommand(UUID orderId, long amountCents, String paymentMethod) {
        var payload = objectMapper.createObjectNode()
                .put("amountCents", amountCents)
                .put("currency", "USD")
                .put("paymentMethod", paymentMethod);
        return command("AuthorizePayment", orderId, payload);
    }

    protected EventEnvelope captureCommand(UUID orderId) {
        return command("CapturePayment", orderId, objectMapper.createObjectNode());
    }

    protected EventEnvelope voidCommand(UUID orderId) {
        return command("VoidPayment", orderId, objectMapper.createObjectNode().put("reason", "FRAUD_REJECTED"));
    }

    protected EventEnvelope command(String type, UUID orderId, JsonNode payload) {
        return new EventEnvelope(UUID.randomUUID(), type, 1, orderId, UUID.randomUUID(), Instant.now(), payload);
    }

    /** Replies written to the outbox for the order, oldest first. */
    protected List<EventEnvelope> replies(UUID orderId) {
        return jdbc.query("SELECT payload::text FROM outbox WHERE aggregate_id = ? ORDER BY created_at",
                (rs, i) -> envelopeMapper.fromJson(rs.getString(1)), orderId.toString());
    }

    protected int paymentRows(UUID orderId, String operation) {
        return jdbc.queryForObject("SELECT count(*) FROM payments WHERE order_id = ? AND operation = ?",
                Integer.class, orderId, operation);
    }
}
