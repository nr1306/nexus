package com.nexus.order.messaging;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class ReplyConsumptionGateTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-08T12:00:00Z"));
    private final ReplyConsumptionGate gate = new ReplyConsumptionGate(clock, Duration.ofSeconds(10));

    @Test
    void closedUntilRepliesAreAssigned() {
        clock.advance(Duration.ofMinutes(5));

        assertThat(gate.timeoutsAllowed()).isFalse();
    }

    @Test
    void opensOnlyAfterTheGraceFollowingTheFirstAssignment() {
        gate.onRepliesAssigned();
        clock.advance(Duration.ofSeconds(9));
        assertThat(gate.timeoutsAllowed()).isFalse();

        // A later rebalance doesn't restart the grace period.
        gate.onRepliesAssigned();
        clock.advance(Duration.ofSeconds(1));
        assertThat(gate.timeoutsAllowed()).isTrue();
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
