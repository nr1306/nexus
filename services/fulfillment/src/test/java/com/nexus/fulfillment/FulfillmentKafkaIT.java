package com.nexus.fulfillment;

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
 * Full path: CreateShipment on {@code fulfillment.commands} and OrderCompleted on {@code order.events}
 * → listener → Postgres + outbox → Debezium → events on {@code fulfillment.events}.
 */
class FulfillmentKafkaIT extends FulfillmentIntegrationTest {

    @Test
    void createDeliveredTwiceThenOrderCompletedShipsOnce() throws Exception {
        UUID orderId = UUID.randomUUID();
        EventEnvelope create = createCommand(orderId);

        try (var producer = producer()) {
            send(producer, "fulfillment.commands", orderId, create);
            send(producer, "fulfillment.commands", orderId, create);
        }
        assertThat(eventsFor(orderId, 1, Duration.ZERO)).extracting(EventEnvelope::eventType)
                .containsExactly("ShipmentCreated");
        try (var producer = producer()) {
            send(producer, "order.events", orderId, orderCompleted(orderId, "carol"));
        }

        List<EventEnvelope> events = eventsFor(orderId, 2, Duration.ofSeconds(5));

        assertThat(events).extracting(EventEnvelope::eventType).containsExactly("ShipmentCreated", "ShipmentShipped");
        assertThat(notifications(orderId)).containsExactlyInAnyOrder("ORDER_CONFIRMED", "ORDER_SHIPPED");
    }

    private void send(KafkaProducer<String, String> producer, String topic, UUID orderId, EventEnvelope message) throws Exception {
        producer.send(new ProducerRecord<>(topic, orderId.toString(), envelopeMapper.toJson(message))).get();
    }

    private KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"),
                new StringSerializer(), new StringSerializer());
    }

    /** Collects events keyed by {@code orderId} until {@code expected} arrive, then waits {@code settle} for extras. */
    private List<EventEnvelope> eventsFor(UUID orderId, int expected, Duration settle) {
        List<EventEnvelope> events = new ArrayList<>();
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("fulfillment.events"));
            Runnable poll = () -> consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                if (orderId.toString().equals(r.key())) {
                    events.add(envelopeMapper.fromJson(r.value()));
                }
            });
            await().atMost(Duration.ofMinutes(1)).until(() -> {
                poll.run();
                return events.size() >= expected;
            });
            long deadline = System.nanoTime() + settle.toNanos();
            while (System.nanoTime() < deadline) {
                poll.run();
            }
        }
        return events;
    }
}
