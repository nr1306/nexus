package com.nexus.messaging.dlq;

import com.nexus.messaging.retry.RetryTopicNaming;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Lists and replays this service's DLQ messages (SPEC.md §7: recovery is an operation, not manual SQL).
 *
 * <p>Pending messages are those after the committed offset of the {@code <group>.dlq-replay} consumer
 * group. {@link #replayAll} republishes them to their source topic and commits; {@link #replayOne}
 * republishes a single record without moving the offset. Replays go through normal consumption, so
 * consumers' idempotency makes replaying an already-processed message harmless. Spring's retry/DLQ
 * headers are dropped so the message starts a fresh retry cycle; all other headers (traceparent, id,
 * eventType) are kept.
 */
public class DlqAdmin {

    public record DlqMessage(int partition, long offset, String key, String eventId, String eventType,
                             String exception, String exceptionMessage, String value) {
    }

    private static final Duration POLL = Duration.ofMillis(500);

    private final String group;
    private final ConsumerFactory<String, String> consumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;

    public DlqAdmin(String group, ConsumerFactory<String, String> consumerFactory,
                    KafkaTemplate<String, String> kafkaTemplate, KafkaAdmin kafkaAdmin) {
        this.group = group;
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaAdmin = kafkaAdmin;
    }

    /** DLQ topics belonging to this service's consumer group. */
    public List<String> topics() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            return admin.listTopics().names().get(30, TimeUnit.SECONDS).stream()
                    .filter(t -> t.endsWith(RetryTopicNaming.dlqSuffix(group)))
                    .sorted()
                    .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Cannot list topics", e);
        }
    }

    public synchronized List<DlqMessage> pending(String dlqTopic, int limit) {
        requireOwnDlq(dlqTopic);
        try (Consumer<String, String> consumer = replayConsumer()) {
            return readPending(consumer, dlqTopic, limit).stream().map(DlqAdmin::toMessage).toList();
        }
    }

    /** Republishes every pending message to its source topic, then marks them replayed. */
    public synchronized int replayAll(String dlqTopic) {
        requireOwnDlq(dlqTopic);
        try (Consumer<String, String> consumer = replayConsumer()) {
            List<ConsumerRecord<String, String>> records = readPending(consumer, dlqTopic, Integer.MAX_VALUE);
            Map<TopicPartition, OffsetAndMetadata> next = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                republish(dlqTopic, record);
                next.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            }
            if (!next.isEmpty()) {
                consumer.commitSync(next);
            }
            return records.size();
        }
    }

    /** Republishes one message; it stays pending (the replay offset doesn't move). */
    public synchronized boolean replayOne(String dlqTopic, int partition, long offset) {
        requireOwnDlq(dlqTopic);
        try (Consumer<String, String> consumer = replayConsumer()) {
            TopicPartition tp = new TopicPartition(dlqTopic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, offset);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(POLL)) {
                    if (record.offset() == offset) {
                        republish(dlqTopic, record);
                        return true;
                    }
                    if (record.offset() > offset) {
                        return false;
                    }
                }
            }
            return false;
        }
    }

    private List<ConsumerRecord<String, String>> readPending(Consumer<String, String> consumer, String dlqTopic, int limit) {
        List<TopicPartition> partitions = consumer.partitionsFor(dlqTopic, Duration.ofSeconds(10)).stream()
                .map(p -> new TopicPartition(dlqTopic, p.partition()))
                .toList();
        consumer.assign(partitions);
        Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new java.util.HashSet<>(partitions));
        for (TopicPartition tp : partitions) {
            OffsetAndMetadata c = committed.get(tp);
            if (c == null) {
                consumer.seekToBeginning(List.of(tp));
            } else {
                consumer.seek(tp, c.offset());
            }
        }
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (records.size() < limit && !caughtUp(consumer, partitions, end) && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(POLL)) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                if (record.offset() < end.get(tp) && records.size() < limit) {
                    records.add(record);
                }
            }
        }
        return records;
    }

    private static boolean caughtUp(Consumer<?, ?> consumer, List<TopicPartition> partitions, Map<TopicPartition, Long> end) {
        return partitions.stream().allMatch(tp -> consumer.position(tp) >= end.get(tp));
    }

    private void republish(String dlqTopic, ConsumerRecord<String, String> record) {
        RecordHeaders headers = new RecordHeaders();
        for (Header h : record.headers()) {
            if (!h.key().startsWith("kafka_") && !h.key().startsWith("retry_topic-")) {
                headers.add(h);
            }
        }
        String source = dlqTopic.substring(0, dlqTopic.length() - RetryTopicNaming.dlqSuffix(group).length());
        try {
            kafkaTemplate.send(new ProducerRecord<>(source, null, record.key(), record.value(), headers)).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Replay to " + source + " failed", e);
        }
    }

    private Consumer<String, String> replayConsumer() {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.GROUP_ID_CONFIG, group + ".dlq-replay");
        overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerFactory.createConsumer(group + ".dlq-replay", null, null, overrides);
    }

    private void requireOwnDlq(String dlqTopic) {
        if (!dlqTopic.endsWith(RetryTopicNaming.dlqSuffix(group))) {
            throw new IllegalArgumentException(dlqTopic + " is not a DLQ of consumer group " + group);
        }
    }

    private static DlqMessage toMessage(ConsumerRecord<String, String> r) {
        return new DlqMessage(r.partition(), r.offset(), r.key(),
                DeadLetterRecorder.header(r, "id"),
                DeadLetterRecorder.header(r, "eventType"),
                DeadLetterRecorder.cause(r),
                DeadLetterRecorder.header(r, KafkaHeaders.EXCEPTION_MESSAGE),
                r.value());
    }
}
