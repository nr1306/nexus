package com.nexus.order.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds back saga timeouts until this pod is reading replies (ADR 0008). After a restart, deadlines
 * that passed while Order was down are already overdue, but their replies may be waiting unread in
 * Kafka until the consumer gets its partitions. Timeouts are allowed once the reply listener has
 * been assigned partitions and {@code startup-grace} has passed, so it can work through the backlog.
 */
@Component
public class ReplyConsumptionGate {

    private static final Logger log = LoggerFactory.getLogger(ReplyConsumptionGate.class);

    private final Clock clock;
    private final Duration startupGrace;
    private final AtomicReference<Instant> firstAssignedAt = new AtomicReference<>();

    public ReplyConsumptionGate(Clock clock,
                                @Value("${nexus.order.timeout-sweeper.startup-grace:PT10S}") Duration startupGrace) {
        this.clock = clock;
        this.startupGrace = startupGrace;
    }

    /** Called when the reply listener is first assigned partitions of the reply topics. */
    void onRepliesAssigned() {
        if (firstAssignedAt.compareAndSet(null, clock.instant())) {
            log.info("Reply topics assigned; saga timeouts enabled after {}", startupGrace);
        }
    }

    public boolean timeoutsAllowed() {
        Instant assignedAt = firstAssignedAt.get();
        return assignedAt != null && !clock.instant().isBefore(assignedAt.plus(startupGrace));
    }
}
