package com.nexus.inventory;

import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
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
 * Full path: command on {@code inventory.commands} → listener → Postgres + outbox → Debezium →
 * reply on {@code inventory.events}.
 */
class InventoryKafkaIT extends InventoryIntegrationTest {

    @Autowired
    EnvelopeMapper envelopeMapper;

    @Test
    void reserveCommandDeliveredTwiceProducesOneReservationAndOneReply() throws Exception {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        EventEnvelope command = reserveCommand(orderId, Map.of(sku, 3));

        try (var producer = producer()) {
            var record = new ProducerRecord<>("inventory.commands", orderId.toString(), envelopeMapper.toJson(command));
            producer.send(record).get();
            producer.send(record).get();
        }

        List<EventEnvelope> replies = repliesFor(orderId, 1, Duration.ofSeconds(8));

        assertThat(replies).hasSize(1);
        EventEnvelope reply = replies.getFirst();
        assertThat(reply.eventType()).isEqualTo("InventoryReserved");
        assertThat(reply.sagaId()).isEqualTo(command.sagaId());
        assertThat(available(sku)).isEqualTo(7);
        assertThat(reserved(sku)).isEqualTo(3);
    }

    @Test
    void releaseCommandReturnsStockAndReplies() throws Exception {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();

        try (var producer = producer()) {
            producer.send(new ProducerRecord<>("inventory.commands", orderId.toString(),
                    envelopeMapper.toJson(reserveCommand(orderId, Map.of(sku, 2))))).get();
            producer.send(new ProducerRecord<>("inventory.commands", orderId.toString(),
                    envelopeMapper.toJson(releaseCommand(orderId)))).get();
        }

        List<EventEnvelope> replies = repliesFor(orderId, 2, Duration.ofSeconds(3));

        assertThat(replies).extracting(EventEnvelope::eventType)
                .containsExactly("InventoryReserved", "InventoryReleased");
        assertThat(available(sku)).isEqualTo(10);
        assertThat(reserved(sku)).isZero();
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
            consumer.subscribe(List.of("inventory.events"));
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
