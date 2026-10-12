package com.nexus.reconciliation.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param settleWindow       an order is checked only once no event for it has arrived for this long
 * @param stuckAfter         a non-terminal saga quiet for this long is SAGA_STUCK (SPEC §7 invariant 4)
 * @param repairRetryAfter   a repair whose finding is still open after this long is re-sent
 * @param maxRepairAttempts  sends of a repair command before leaving the finding to an operator
 * @param topicPartitions    partitions for the topics this service declares (SPEC.md §5)
 * @param topicReplicas      replication factor for those topics; 1 locally
 */
@ConfigurationProperties("nexus.reconciliation")
public record ReconciliationProperties(
        @DefaultValue("PT2M") Duration settleWindow,
        @DefaultValue("PT5M") Duration stuckAfter,
        @DefaultValue("PT2M") Duration repairRetryAfter,
        @DefaultValue("3") int maxRepairAttempts,
        @DefaultValue("12") int topicPartitions,
        @DefaultValue("1") short topicReplicas) {
}
