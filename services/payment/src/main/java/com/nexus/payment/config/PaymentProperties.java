package com.nexus.payment.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param gateway         payment provider adapter: {@code mock} (Stripe test mode is added in Phase 2)
 * @param mockLatency     artificial delay per mock provider call, to make load tests realistic
 * @param topicPartitions partitions for payment topics (SPEC.md §5)
 * @param topicReplicas   replication factor for payment topics; 1 locally
 */
@ConfigurationProperties("nexus.payment")
public record PaymentProperties(
        @DefaultValue("mock") String gateway,
        @DefaultValue("PT0S") Duration mockLatency,
        @DefaultValue("12") int topicPartitions,
        @DefaultValue("1") short topicReplicas) {
}
