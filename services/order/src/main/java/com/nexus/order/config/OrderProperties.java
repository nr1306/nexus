package com.nexus.order.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param stepTimeout             how long a saga step waits for its reply (SPEC.md §4: 30 s)
 * @param maxStepAttempts         sends of a forward command before compensating (2 = retry once)
 * @param maxCompensationAttempts sends of a compensation command before NEEDS_ATTENTION
 * @param sweepBatchSize          overdue sagas handled per sweep
 * @param topicPartitions         partitions for order.events (SPEC.md §5)
 * @param topicReplicas           replication factor for order.events; 1 locally
 */
@ConfigurationProperties("nexus.order")
public record OrderProperties(
        @DefaultValue("PT30S") Duration stepTimeout,
        @DefaultValue("2") int maxStepAttempts,
        @DefaultValue("5") int maxCompensationAttempts,
        @DefaultValue("100") int sweepBatchSize,
        @DefaultValue("12") int topicPartitions,
        @DefaultValue("1") short topicReplicas) {
}
