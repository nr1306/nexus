package com.nexus.order.fraud;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/** Retries fraud checks that couldn't get a verdict yet. Off when tests drive checks explicitly. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexus.order.fraud.auto-run", havingValue = "true", matchIfMissing = true)
class FraudCheckScheduling {

    private final FraudCheckRunner runner;

    FraudCheckScheduling(FraudCheckRunner runner) {
        this.runner = runner;
    }

    @Scheduled(fixedDelayString = "${nexus.order.fraud.poll-interval:PT1S}")
    void poll() {
        runner.runPending();
    }
}
