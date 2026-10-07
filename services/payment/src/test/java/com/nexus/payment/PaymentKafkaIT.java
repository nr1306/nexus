package com.nexus.payment;

import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.payment.gateway.MockPaymentGateway;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Full path: command on {@code payment.commands} → listener → Postgres + outbox → Debezium →
 * reply on {@code payment.events}.
 */
class PaymentKafkaIT extends PaymentIntegrationTest {

    @Autowired
    MockPaymentGateway gateway;

    @Test
    void authorizeDeliveredTwiceThenCaptureChargesOnce() throws Exception {
        UUID orderId = UUID.randomUUID();
        EventEnvelope authorize = authorizeCommand(orderId, 2500, "pm_card_visa");

        try (var producer = producer()) {
            send(producer, orderId, authorize);
            send(producer, orderId, authorize);
            send(producer, orderId, captureCommand(orderId));
        }

        List<EventEnvelope> replies = repliesFor(orderId, 2, Duration.ofSeconds(8));

        assertThat(replies).extracting(EventEnvelope::eventType).containsExactly("PaymentAuthorized", "PaymentCaptured");
        assertThat(replies.get(1).payload().get("amountCents").asLong()).isEqualTo(2500);
        assertThat(gateway.calls(orderId + ":AUTHORIZE")).isEqualTo(1);
        assertThat(gateway.calls(orderId + ":CAPTURE")).isEqualTo(1);
    }

    private void send(KafkaProducer<String, String> producer, UUID orderId, EventEnvelope command) throws Exception {
        producer.send(new ProducerRecord<>("payment.commands", orderId.toString(), envelopeMapper.toJson(command))).get();
    }

    private KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"),
                new StringSerializer(), new StringSerializer());
    }

    /** Collects replies keyed by {@code orderId} until {@code expected} arrive, then waits {@code settle} for extras. */
    private List<EventEnvelope> repliesFor(UUID orderId, int expected, Duration settle) {
        List<EventEnvelope> replies = new ArrayList<>();
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("payment.events"));
            Runnable poll = () -> consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                if (orderId.toString().equals(r.key())) {
                    replies.add(envelopeMapper.fromJson(r.value()));
                }
            });
            await().atMost(Duration.ofMinutes(1)).until(() -> {
                poll.run();
                return replies.size() >= expected;
            });
            long deadline = System.nanoTime() + settle.toNanos();
            while (System.nanoTime() < deadline) {
                poll.run();
            }
        }
        return replies;
    }
}
