package com.nexus.reconciliation.finding;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/** Runs the checker periodically (SPEC §7: every 60 s). Disabled in tests that run it explicitly. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexus.reconciliation.checker.enabled", havingValue = "true", matchIfMissing = true)
class ReconciliationScheduling {

    private final ReconciliationChecker checker;

    ReconciliationScheduling(ReconciliationChecker checker) {
        this.checker = checker;
    }

    @Scheduled(fixedDelayString = "${nexus.reconciliation.checker.interval:PT60S}")
    void check() {
        checker.check();
    }
}
