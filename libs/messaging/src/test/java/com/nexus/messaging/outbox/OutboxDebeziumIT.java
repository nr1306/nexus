package com.nexus.messaging.outbox;

import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.support.TestEnvelopes;
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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres")
            .withDatabaseName("order_db")
            .withUsername("nexus")
            .withPassword("nexus")
            .withCommand("postgres", "-c", "wal_level=logical");

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.2")
            .withNetwork(NETWORK)
            .withListener("kafka:19092");

    @Container
    private static final GenericContainer<?> CONNECT = new GenericContainer<>("quay.io/debezium/connect:3.7.0.Final")
            .withNetwork(NETWORK)
            .withExposedPorts(8083)
            .withEnv(Map.ofEntries(
                    Map.entry("BOOTSTRAP_SERVERS", "kafka:19092"),
                    Map.entry("GROUP_ID", "nexus-connect-it"),
                    Map.entry("CONFIG_STORAGE_TOPIC", "_connect_configs"),
                    Map.entry("OFFSET_STORAGE_TOPIC", "_connect_offsets"),
                    Map.entry("STATUS_STORAGE_TOPIC", "_connect_status"),
                    Map.entry("CONFIG_STORAGE_REPLICATION_FACTOR", "1"),
                    Map.entry("OFFSET_STORAGE_REPLICATION_FACTOR", "1"),
                    Map.entry("STATUS_STORAGE_REPLICATION_FACTOR", "1"),
                    Map.entry("CONNECT_CONFIG_PROVIDERS", "env"),
                    Map.entry("CONNECT_CONFIG_PROVIDERS_ENV_CLASS",
                            "org.apache.kafka.common.config.provider.EnvVarConfigProvider"),
                    Map.entry("POSTGRES_USER", "nexus"),
                    Map.entry("POSTGRES_PASSWORD", "nexus")))
            .dependsOn(KAFKA, POSTGRES)
            .waitingFor(Wait.forHttp("/connectors").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)));

    private static JdbcTemplate jdbc;
    private static TransactionTemplate tx;
    private static final EnvelopeMapper ENVELOPE_MAPPER = new EnvelopeMapper(TestEnvelopes.OBJECT_MAPPER);

    @BeforeAll
    static void migrateAndRegisterConnector() throws Exception {
        var dataSource = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

        String config = Files.readString(Path.of(System.getProperty("nexus.connectorTemplate")))
                .replace("__SERVICE__", "order");
        String connectUrl = "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083);
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(connectUrl + "/connectors/order-outbox/config"))
                        .header("Content-Type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(config))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isIn(200, 201);

        // Streaming starts once the replication slot is active; rows written after that arrive via the WAL.
        await().atMost(Duration.ofMinutes(2)).until(() -> jdbc.queryForObject(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'order_outbox' AND active",
                Integer.class) == 1);
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
