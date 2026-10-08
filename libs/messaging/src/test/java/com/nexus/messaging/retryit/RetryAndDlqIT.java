package com.nexus.messaging.retryit;

import com.nexus.messaging.dlq.DlqAdmin;
import com.nexus.messaging.dlq.DlqAdmin.DlqMessage;
import com.nexus.messaging.testing.NexusContainers;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Retry topics, DLQ and replay against real Kafka, with short delays (200/400/600 ms) standing in for 1/5/30 s.
 */
@SpringBootTest(classes = RetryTestApplication.class, properties = {
        "spring.kafka.consumer.group-id=retry-it",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "nexus.messaging.retry.delays=200ms,400ms,600ms",
        "nexus.messaging.retry.partitions=3"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RetryAndDlqIT {

    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private static final String DLQ = "retry.test.retry-it.dlq";

    private static final Network NETWORK = Network.newNetwork();
    private static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(NETWORK, "retry_db");
    private static final KafkaContainer KAFKA = NexusContainers.kafka(NETWORK)
            .withEnv("KAFKA_NUM_PARTITIONS", "3");

    static {
        Startables.deepStart(POSTGRES, KAFKA).join();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    FlakyListener listener;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    DlqAdmin dlqAdmin;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    @Order(1)
    void transientFailureIsRetriedThroughDelayedRetryTopics() throws Exception {
        String id = UUID.randomUUID().toString();

        send("fail2:" + id);

        await().atMost(Duration.ofSeconds(30)).until(() -> listener.succeeded.containsKey(id));
        List<FlakyListener.Attempt> attempts = listener.attempts.get(id);
        assertThat(attempts).extracting(FlakyListener.Attempt::topic)
                .containsExactly("retry.test", "retry.test.retry-it.retry-200ms", "retry.test.retry-it.retry-400ms");
        assertThat(attempts.get(1).receivedAtMillis() - attempts.get(0).receivedAtMillis()).isGreaterThanOrEqualTo(200);
        assertThat(attempts.get(2).receivedAtMillis() - attempts.get(1).receivedAtMillis()).isGreaterThanOrEqualTo(400);
        assertThat(attempts).allMatch(a -> TRACEPARENT.equals(a.traceparent()));
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            assertThat(admin.listTopics().names().get()).contains(
                    "retry.test.retry-it.retry-200ms", "retry.test.retry-it.retry-400ms",
                    "retry.test.retry-it.retry-600ms", DLQ);
        }
    }

    @Test
    @Order(2)
    void exhaustedRetriesLandInDlqWithOriginalHeaders() throws Exception {
        String id = UUID.randomUUID().toString();

        send("always:" + id);

        await().atMost(Duration.ofSeconds(30)).until(() -> pendingIds().contains(id));
        assertThat(listener.attempts.get(id)).hasSize(4);
        DlqMessage message = pending(id);
        assertThat(message.exceptionMessage()).contains("dependency down");
        await().atMost(Duration.ofSeconds(10)).until(() -> dlqCount() >= 1);
    }

    @Test
    @Order(3)
    void permanentFailureSkipsRetryTopics() throws Exception {
        String id = UUID.randomUUID().toString();

        send("invalid:" + id);

        await().atMost(Duration.ofSeconds(30)).until(() -> pendingIds().contains(id));
        assertThat(listener.attempts.get(id)).hasSize(1);
        assertThat(pending(id).exception()).isEqualTo("com.nexus.messaging.envelope.InvalidEnvelopeException");
        assertThat(pending(id).exceptionMessage()).contains("bad envelope");
    }

    @Test
    @Order(4)
    void replayAllRepublishesWithTraceparentAndClearsPending() {
        String id = pendingIds().stream()
                .filter(i -> listener.attempts.get(i).size() == 4)
                .findFirst().orElseThrow();
        listener.heal();

        int replayed = dlqAdmin.replayAll(DLQ);

        assertThat(replayed).isGreaterThanOrEqualTo(2);
        await().atMost(Duration.ofSeconds(30)).until(() -> listener.succeeded.containsKey(id));
        List<FlakyListener.Attempt> attempts = listener.attempts.get(id);
        FlakyListener.Attempt replay = attempts.getLast();
        assertThat(replay.topic()).isEqualTo("retry.test");
        assertThat(replay.traceparent()).isEqualTo(TRACEPARENT);
        // The permanently invalid message goes back to the DLQ; the healed one doesn't.
        await().atMost(Duration.ofSeconds(30)).until(() -> !pendingIds().isEmpty());
        assertThat(pendingIds()).doesNotContain(id);
        assertThat(dlqAdmin.topics()).containsExactly(DLQ);
    }

    private void send(String value) throws Exception {
        var record = new ProducerRecord<String, String>(FlakyListener.TOPIC, UUID.randomUUID().toString(), value);
        record.headers().add("traceparent", TRACEPARENT.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get();
    }

    private List<String> pendingIds() {
        return dlqAdmin.pending(DLQ, 100).stream().map(m -> m.value().split(":", 2)[1]).toList();
    }

    private DlqMessage pending(String id) {
        return dlqAdmin.pending(DLQ, 100).stream().filter(m -> m.value().endsWith(id)).findFirst().orElseThrow();
    }

    private double dlqCount() {
        var counter = meterRegistry.find("dlq_messages").tag("topic", "retry.test").counter();
        return counter == null ? 0 : counter.count();
    }
}
