package com.nexus.inventory.reservation;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/** Runs the expiry sweeper periodically. Disabled in tests that drive expiry explicitly. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexus.inventory.expiry-sweeper.enabled", havingValue = "true", matchIfMissing = true)
class ReservationExpiryScheduling {

    private final ReservationExpirySweeper sweeper;

    ReservationExpiryScheduling(ReservationExpirySweeper sweeper) {
        this.sweeper = sweeper;
    }

    @Scheduled(fixedDelayString = "${nexus.inventory.expiry-sweeper.interval:PT10S}")
    void sweep() {
        sweeper.sweep();
    }
}
