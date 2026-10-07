package com.nexus.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.testing.NexusContainers;
import com.nexus.messaging.testing.OutboxConnectors;
import com.nexus.order.order.OrderService;
import com.nexus.order.order.PlaceOrderRequest;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
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
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * Shared stack for Order integration tests (see {@code InventoryIntegrationTest} for the pattern).
 * The timeout sweeper is disabled; tests trigger it explicitly.
 */
@SpringBootTest(properties = "nexus.order.timeout-sweeper.enabled=false")
@AutoConfigureMockMvc
public abstract class OrderIntegrationTest {

    private static final Network NETWORK = Network.newNetwork();
    protected static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(NETWORK, "order_db");
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

    @Autowired
    protected OrderService orderService;

    @BeforeEach
    void registerConnectorOnce() {
        synchronized (OrderIntegrationTest.class) {
            if (!connectorRegistered) {
                OutboxConnectors.register(CONNECT, jdbc, "order");
                connectorRegistered = true;
            }
        }
    }

    protected static PlaceOrderRequest orderRequest(String paymentMethod) {
        return new PlaceOrderRequest("customer-1",
                List.of(new PlaceOrderRequest.Line("SKU-0001", 2, 1_500L),
                        new PlaceOrderRequest.Line("SKU-0002", 1, 2_000L)),
                "USD", paymentMethod);
    }

    protected UUID placeOrder() {
        return orderService.placeOrder(UUID.randomUUID().toString(), orderRequest("pm_card_visa")).order().orderId();
    }

    /** Everything the order service wrote to its outbox for this order, oldest first. */
    protected List<Written> outbox(UUID orderId) {
        return jdbc.query("SELECT topic, payload::text FROM outbox WHERE aggregate_id = ?",
                        (rs, i) -> new Written(rs.getString(1), envelopeMapper.fromJson(rs.getString(2))),
                        orderId.toString())
                .stream()
                .sorted(Comparator.comparing((Written w) -> w.envelope().occurredAt()))
                .toList();
    }

    /** Command types sent to Inventory and Payment, oldest first. */
    protected List<String> commands(UUID orderId) {
        return outbox(orderId).stream()
                .filter(w -> w.topic().endsWith(".commands"))
                .map(w -> w.envelope().eventType())
                .toList();
    }

    protected List<String> orderEvents(UUID orderId) {
        return outbox(orderId).stream()
                .filter(w -> w.topic().equals("order.events"))
                .map(w -> w.envelope().eventType())
                .toList();
    }

    protected EventEnvelope lastCommand(UUID orderId) {
        return outbox(orderId).stream()
                .filter(w -> w.topic().endsWith(".commands"))
                .reduce((a, b) -> b)
                .orElseThrow()
                .envelope();
    }

    /** A reply from Inventory or Payment to the given command. */
    protected EventEnvelope replyTo(EventEnvelope command, String replyType, JsonNode payload) {
        return new EventEnvelope(UUID.randomUUID(), replyType, 1, command.orderId(), command.sagaId(), Instant.now(), payload);
    }

    protected EventEnvelope replyTo(EventEnvelope command, String replyType) {
        return replyTo(command, replyType, objectMapper.createObjectNode());
    }

    protected EventEnvelope failureReplyTo(EventEnvelope command, String replyType, String reason) {
        return replyTo(command, replyType, objectMapper.createObjectNode().put("reason", reason));
    }

    protected String state(UUID orderId) {
        return jdbc.queryForObject("SELECT state FROM saga_instances WHERE order_id = ?", String.class, orderId);
    }

    protected String failureReason(UUID orderId) {
        return jdbc.queryForObject("SELECT failure_reason FROM saga_instances WHERE order_id = ?", String.class, orderId);
    }

    protected void expireCurrentStep(UUID orderId) {
        jdbc.update("UPDATE saga_instances SET step_deadline = now() - interval '1 second' WHERE order_id = ?", orderId);
    }

    protected record Written(String topic, EventEnvelope envelope) {
    }
}
