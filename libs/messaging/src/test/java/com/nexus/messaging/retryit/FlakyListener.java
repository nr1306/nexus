package com.nexus.messaging.retryit;

import com.nexus.messaging.envelope.InvalidEnvelopeException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Value format {@code <mode>:<id>}:
 * {@code fail2} fails twice then succeeds; {@code always} fails until {@link #heal()};
 * {@code invalid} fails permanently.
 */
@Component
class FlakyListener {

    static final String TOPIC = "retry.test";

    record Attempt(String topic, long receivedAtMillis, String traceparent) {
    }

    final Map<String, List<Attempt>> attempts = new ConcurrentHashMap<>();
    final Map<String, Boolean> succeeded = new ConcurrentHashMap<>();
    private volatile boolean healed;

    @KafkaListener(topics = TOPIC, groupId = "retry-it")
    void onMessage(ConsumerRecord<String, String> record) {
        String[] parts = record.value().split(":", 2);
        String mode = parts[0];
        String id = parts[1];
        Header tp = record.headers().lastHeader("traceparent");
        List<Attempt> seen = attempts.computeIfAbsent(id, k -> new CopyOnWriteArrayList<>());
        seen.add(new Attempt(record.topic(), System.currentTimeMillis(),
                tp == null ? null : new String(tp.value(), StandardCharsets.UTF_8)));

        switch (mode) {
            case "fail2" -> {
                if (seen.size() <= 2) {
                    throw new IllegalStateException("transient failure " + seen.size());
                }
            }
            case "always" -> {
                if (!healed) {
                    throw new IllegalStateException("dependency down");
                }
            }
            case "invalid" -> throw new InvalidEnvelopeException("bad envelope", null);
            default -> { }
        }
        succeeded.put(id, true);
    }

    void heal() {
        healed = true;
    }
}
