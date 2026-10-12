package com.nexus.reconciliation;

import com.nexus.messaging.envelope.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Full path: events on order.events / payment.events (one delivered twice) → projection → finding →
 * RefundPayment through the outbox and Debezium onto {@code payment.commands}.
 */
class ReconciliationKafkaIT extends ReconciliationIntegrationTest {

    @Test
    void cancelledButChargedOverKafkaPublishesOneRefund() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        EventEnvelope created = event("OrderCreated", orderId, sagaId);
        EventEnvelope cancelled = event("OrderCancelled", orderId, sagaId,
                objectMapper.createObjectNode().put("reason", "CARD_DECLINED"));
        EventEnvelope captured = event("PaymentCaptured", orderId, sagaId,
                objectMapper.createObjectNode().put("amountCents", 1000).put("currency", "USD"));

        try (var producer = producer()) {
            send(producer, "order.events", created);
            send(producer, "order.events", cancelled);
            send(producer, "payment.events", captured);
            send(producer, "payment.events", captured);
        }
        await().atMost(Duration.ofMinutes(1)).until(() -> jdbc.queryForObject(
                "SELECT count(*) FROM processed_events WHERE event_id IN (?, ?, ?)", Long.class,
                created.eventId(), cancelled.eventId(), captured.eventId()) == 3);
        clock.advance(PAST_SETTLE);
        checker.check();

        try (var consumer = consumer("payment.commands")) {
            List<EventEnvelope> commands = new ArrayList<>();
            await().atMost(Duration.ofMinutes(1)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (orderId.toString().equals(r.key())) {
                        commands.add(envelopeMapper.fromJson(r.value()));
                    }
                });
                return !commands.isEmpty();
            });
            consumer.poll(Duration.ofSeconds(3)).forEach(r -> {
                if (orderId.toString().equals(r.key())) {
                    commands.add(envelopeMapper.fromJson(r.value()));
                }
            });
            assertThat(commands).singleElement().satisfies(c -> {
                assertThat(c.eventType()).isEqualTo("RefundPayment");
                assertThat(c.sagaId()).isEqualTo(sagaId);
            });
        }
        assertThat(openFindings(orderId)).containsExactly("CANCELLED_BUT_CHARGED");
    }

    private void send(KafkaProducer<String, String> producer, String topic, EventEnvelope event) throws Exception {
        producer.send(new ProducerRecord<>(topic, event.orderId().toString(), envelopeMapper.toJson(event))).get();
    }

    private KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"),
                new StringSerializer(), new StringSerializer());
    }

    private KafkaConsumer<String, String> consumer(String topic) {
        var consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }
}
