package com.nexus.order.saga;

import com.nexus.order.messaging.ReplyConsumptionGate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Runs the timeout sweeper periodically, once {@link ReplyConsumptionGate} allows it. Disabled in
 * tests that drive timeouts explicitly.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexus.order.timeout-sweeper.enabled", havingValue = "true", matchIfMissing = true)
class SagaTimeoutScheduling {

    private final SagaTimeoutSweeper sweeper;
    private final ReplyConsumptionGate gate;

    SagaTimeoutScheduling(SagaTimeoutSweeper sweeper, ReplyConsumptionGate gate) {
        this.sweeper = sweeper;
        this.gate = gate;
    }

    @Scheduled(fixedDelayString = "${nexus.order.timeout-sweeper.interval:PT1S}")
    void sweep() {
        if (gate.timeoutsAllowed()) {
            sweeper.sweep();
        }
    }
}
