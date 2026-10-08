package com.nexus.messaging.retry;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

/**
 * Non-blocking retry topics and DLQ for every {@code @KafkaListener} in a service (CLAUDE.md rule 10, ADR 0004).
 *
 * @param enabled    turn retry topics off (failures then use Spring Kafka's default error handler)
 * @param delays     delay before each retry; one retry topic per delay (SPEC.md §7: 1 s, 5 s, 30 s)
 * @param partitions partitions for retry and DLQ topics; must be ≥ the source topic's (records keep their partition)
 * @param replicas   replication factor for retry and DLQ topics
 */
@ConfigurationProperties("nexus.messaging.retry")
public record RetryProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue({"1s", "5s", "30s"}) List<Duration> delays,
        @DefaultValue("12") int partitions,
        @DefaultValue("1") short replicas) {
}
