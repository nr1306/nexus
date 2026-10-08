package com.nexus.inventory.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param reservationTtl how long a hold lasts before the sweeper may release it (SPEC.md §7)
 * @param expirySweepBatchSize orders the expiry sweeper handles per run
 * @param topicPartitions partitions for inventory topics (SPEC.md §5)
 * @param topicReplicas replication factor for inventory topics; 1 locally
 */
@ConfigurationProperties("nexus.inventory")
public record InventoryProperties(
        @DefaultValue("PT10M") Duration reservationTtl,
        @DefaultValue("100") int expirySweepBatchSize,
        @DefaultValue("12") int topicPartitions,
        @DefaultValue("1") short topicReplicas) {
}
