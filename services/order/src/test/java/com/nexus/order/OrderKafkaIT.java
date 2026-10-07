package com.nexus.order;

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
 * Order over real Kafka: commands leave via outbox + Debezium; replies arrive on inventory.events /
 * payment.events, playing the role of Inventory and Payment.
 */
class OrderKafkaIT extends OrderIntegrationTest {

    @Test
    void sagaCompletesOverKafka() throws Exception {
        UUID orderId = placeOrder();
        try (var commands = consumer(List.of("inventory.commands", "payment.commands"));
             var producer = producer()) {

            EventEnvelope reserve = awaitCommand(commands, orderId, "ReserveInventory");
            send(producer, "inventory.events", replyTo(reserve, "InventoryReserved"));

            EventEnvelope authorize = awaitCommand(commands, orderId, "AuthorizePayment");
            send(producer, "payment.events", replyTo(authorize, "PaymentAuthorized"));

            EventEnvelope capture = awaitCommand(commands, orderId, "CapturePayment");
            send(producer, "payment.events", replyTo(capture, "PaymentCaptured"));

            awaitCommand(commands, orderId, "CommitInventory");
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> state(orderId).equals("COMPLETED"));
    }

    private EventEnvelope awaitCommand(KafkaConsumer<String, String> consumer, UUID orderId, String type) {
        List<EventEnvelope> found = new ArrayList<>();
        await().atMost(Duration.ofMinutes(1)).until(() -> {
            consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                EventEnvelope e = envelopeMapper.fromJson(r.value());
                if (orderId.toString().equals(r.key()) && e.eventType().equals(type)) {
                    found.add(e);
                }
            });
            return !found.isEmpty();
        });
        assertThat(found).hasSize(1);
        return found.getFirst();
    }

    private void send(KafkaProducer<String, String> producer, String topic, EventEnvelope reply) throws Exception {
        producer.send(new ProducerRecord<>(topic, reply.orderId().toString(), envelopeMapper.toJson(reply))).get();
    }

    private KafkaConsumer<String, String> consumer(List<String> topics) {
        var consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(topics);
        return consumer;
    }

    private KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"),
                new StringSerializer(), new StringSerializer());
    }
}
