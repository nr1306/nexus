package com.nexus.order.saga;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/** Runs the timeout sweeper periodically. Disabled in tests that drive timeouts explicitly. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexus.order.timeout-sweeper.enabled", havingValue = "true", matchIfMissing = true)
class SagaTimeoutScheduling {

    private final SagaTimeoutSweeper sweeper;

    SagaTimeoutScheduling(SagaTimeoutSweeper sweeper) {
        this.sweeper = sweeper;
    }

    @Scheduled(fixedDelayString = "${nexus.order.timeout-sweeper.interval:PT1S}")
    void sweep() {
        sweeper.sweep();
    }
}
