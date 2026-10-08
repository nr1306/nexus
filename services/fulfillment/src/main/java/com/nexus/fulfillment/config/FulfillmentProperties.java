package com.nexus.fulfillment.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topicPartitions partitions for fulfillment topics (SPEC.md §5)
 * @param topicReplicas replication factor for fulfillment topics; 1 locally
 */
@ConfigurationProperties("nexus.fulfillment")
public record FulfillmentProperties(
        @DefaultValue("12") int topicPartitions,
        @DefaultValue("1") short topicReplicas) {
}
