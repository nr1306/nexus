package com.nexus.messaging.outbox;

import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.support.TestEnvelopes;
import com.nexus.messaging.testing.NexusContainers;
import com.nexus.messaging.testing.OutboxConnectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the outbox path end to end with the same images and connector config as the local stack
 * ({@code deploy/compose}, {@code deploy/connect/outbox-connector.json}).
 */
@Testcontainers
class OutboxDebeziumIT {

    private static final String TOPIC = "inventory.commands";
    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(NETWORK, "order_db");

    @Container
    private static final KafkaContainer KAFKA = NexusContainers.kafka(NETWORK);

    @Container
    private static final GenericContainer<?> CONNECT = NexusContainers.connect(NETWORK).dependsOn(KAFKA, POSTGRES);

    private static JdbcTemplate jdbc;
    private static TransactionTemplate tx;
    private static final EnvelopeMapper ENVELOPE_MAPPER = new EnvelopeMapper(TestEnvelopes.OBJECT_MAPPER);

    @BeforeAll
    static void migrateAndRegisterConnector() {
        var dataSource = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        OutboxConnectors.register(CONNECT, jdbc, "order");
    }

    @Test
    void committedOutboxRowIsPublishedKeyedByOrderIdAndRolledBackRowIsNot() {
        OutboxWriter writer = new OutboxWriter(jdbc, ENVELOPE_MAPPER, () -> Optional.of(TRACEPARENT));
        UUID orderId = UUID.randomUUID();
        EventEnvelope rolledBack = TestEnvelopes.reserveInventory(orderId);
        EventEnvelope committed = TestEnvelopes.reserveInventory(orderId);

        tx.executeWithoutResult(status -> {
            writer.append(TOPIC, "order", rolledBack);
            status.setRollbackOnly();
        });
        tx.executeWithoutResult(status -> writer.append(TOPIC, "order", committed));

        List<ConsumerRecord<String, String>> records = consumeFor(Duration.ofSeconds(10), 1);

        assertThat(records).hasSize(1);
        ConsumerRecord<String, String> record = records.getFirst();
        assertThat(record.key()).isEqualTo(orderId.toString());
        assertThat(ENVELOPE_MAPPER.fromJson(record.value())).isEqualTo(committed);
        assertThat(header(record, "id")).isEqualTo(committed.eventId().toString());
        assertThat(header(record, "eventType")).isEqualTo("ReserveInventory");
        assertThat(header(record, "traceparent")).isEqualTo(TRACEPARENT);
    }

    /** Polls until at least {@code expected} records arrive, then keeps polling briefly to catch extras. */
    private static List<ConsumerRecord<String, String>> consumeFor(Duration settle, int expected) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(TOPIC));
            await().atMost(Duration.ofMinutes(1)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
                return records.size() >= expected;
            });
            long deadline = System.nanoTime() + settle.toNanos();
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        return records;
    }

    private static String header(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
