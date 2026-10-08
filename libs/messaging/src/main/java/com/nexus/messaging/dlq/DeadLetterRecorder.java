package com.nexus.messaging.dlq;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;

/**
 * Handles records arriving on a DLQ: logs them with order context and counts
 * {@code dlq_messages_total{topic}}. The records stay on the DLQ for {@link DlqAdmin} to list and replay.
 */
public class DeadLetterRecorder {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRecorder.class);

    private final MeterRegistry meterRegistry;

    public DeadLetterRecorder(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void onDeadLetter(ConsumerRecord<String, String> record) {
        String originalTopic = header(record, KafkaHeaders.ORIGINAL_TOPIC);
        String topic = originalTopic != null ? originalTopic : record.topic();
        try (var orderId = MDC.putCloseable("orderId", record.key());
             var eventId = MDC.putCloseable("eventId", header(record, "id"))) {
            log.error("Dead-lettered message from {} after {}: {}", topic,
                    cause(record), header(record, KafkaHeaders.EXCEPTION_MESSAGE));
        }
        Counter.builder("dlq_messages")
                .description("Messages that exhausted retries or failed permanently")
                .tag("topic", topic)
                .register(meterRegistry)
                .increment();
    }

    /** The root exception class (the listener's own exception, not Spring's wrapper). */
    static String cause(ConsumerRecord<?, ?> record) {
        String cause = header(record, KafkaHeaders.EXCEPTION_CAUSE_FQCN);
        return cause != null ? cause : header(record, KafkaHeaders.EXCEPTION_FQCN);
    }

    static String header(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
