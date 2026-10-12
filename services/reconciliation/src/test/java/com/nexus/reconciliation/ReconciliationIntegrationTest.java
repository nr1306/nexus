package com.nexus.reconciliation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.testing.NexusContainers;
import com.nexus.messaging.testing.OutboxConnectors;
import com.nexus.reconciliation.finding.ReconciliationChecker;
import com.nexus.reconciliation.messaging.ReconciliationHandler;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Shared stack for Reconciliation integration tests (see {@code InventoryIntegrationTest} for the
 * pattern). The checker runs only when a test calls it, on a clock the test moves forward.
 */
@SpringBootTest(properties = {
        "nexus.reconciliation.checker.enabled=false",
        "nexus.reconciliation.settle-window=PT30S",
        "nexus.reconciliation.stuck-after=PT5M",
        "nexus.reconciliation.repair-retry-after=PT2M",
        "nexus.reconciliation.max-repair-attempts=3"})
@Import(ReconciliationIntegrationTest.TestClockConfig.class)
public abstract class ReconciliationIntegrationTest {

    protected static final Duration PAST_SETTLE = Duration.ofSeconds(31);

    private static final Network NETWORK = Network.newNetwork();
    protected static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(NETWORK, "reconciliation_db");
    protected static final KafkaContainer KAFKA = NexusContainers.kafka(NETWORK);
    protected static final GenericContainer<?> CONNECT = NexusContainers.connect(NETWORK).dependsOn(KAFKA, POSTGRES);

    private static boolean connectorRegistered;

    static {
        Startables.deepStart(POSTGRES, KAFKA, CONNECT).join();
    }

    @TestConfiguration
    static class TestClockConfig {
        @Bean
        @Primary
        MutableClock testClock() {
            return new MutableClock(Instant.now());
        }
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

    @Autowired
    protected MutableClock clock;

    @Autowired
    protected ReconciliationHandler handler;

    @Autowired
    protected ReconciliationChecker checker;

    @BeforeEach
    void registerConnectorOnce() {
        synchronized (ReconciliationIntegrationTest.class) {
            if (!connectorRegistered) {
                OutboxConnectors.register(CONNECT, jdbc, "reconciliation");
                connectorRegistered = true;
            }
        }
    }

    /** An event for the order/saga, occurring now on the test clock. */
    protected EventEnvelope event(String type, UUID orderId, UUID sagaId, JsonNode payload) {
        return new EventEnvelope(UUID.randomUUID(), type, 1, orderId, sagaId, clock.instant(),
                payload != null ? payload : objectMapper.createObjectNode());
    }

    protected EventEnvelope event(String type, UUID orderId, UUID sagaId) {
        return event(type, orderId, sagaId, null);
    }

    /** Applies the events in order, a millisecond apart on the test clock. */
    protected void apply(UUID orderId, UUID sagaId, String... types) {
        for (String type : types) {
            clock.advance(Duration.ofMillis(1));
            handler.handle(event(type, orderId, sagaId, payloadFor(type)));
        }
    }

    private JsonNode payloadFor(String type) {
        return switch (type) {
            case "PaymentRefunded" -> objectMapper.createObjectNode().put("refunded", true);
            case "PaymentVoided" -> objectMapper.createObjectNode().put("voided", true);
            case "ShipmentCancelled" -> objectMapper.createObjectNode().put("cancelled", true);
            default -> objectMapper.createObjectNode();
        };
    }

    /** Open and resolved finding types for the order. */
    protected List<String> findings(UUID orderId) {
        return jdbc.queryForList("SELECT finding_type FROM findings WHERE order_id = ? ORDER BY finding_type",
                String.class, orderId);
    }

    protected List<String> openFindings(UUID orderId) {
        return jdbc.queryForList("SELECT finding_type FROM findings WHERE order_id = ? AND resolved_at IS NULL "
                + "ORDER BY finding_type", String.class, orderId);
    }

    /** Repair commands written to the outbox for the order, oldest first. */
    protected List<EventEnvelope> repairs(UUID orderId) {
        return jdbc.query("SELECT payload::text FROM outbox WHERE aggregate_id = ? ORDER BY created_at",
                (rs, i) -> envelopeMapper.fromJson(rs.getString(1)), orderId.toString());
    }
}
