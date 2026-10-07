package com.nexus.order.saga;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;

/**
 * Saga metrics (SPEC.md §9). Recorded after the transaction commits, so rolled-back transitions don't count.
 */
@Component
public class SagaMetrics {

    private final MeterRegistry registry;
    private final Timer duration;

    public SagaMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.duration = Timer.builder("saga_duration")
                .description("Order accepted to COMPLETED")
                .publishPercentileHistogram()
                .register(registry);
    }

    public void completed(Duration elapsed) {
        afterCommit(() -> {
            registry.counter("saga_completed").increment();
            duration.record(elapsed);
        });
    }

    /** A saga entered COMPENSATING. */
    public void compensating(String reason) {
        afterCommit(() -> counter("saga_compensating", reason).increment());
    }

    /** A compensating saga reached CANCELLED with every step undone. */
    public void compensated(String reason) {
        afterCommit(() -> counter("saga_compensated", reason).increment());
    }

    /** Any saga reached CANCELLED, with or without compensations. */
    public void cancelled(String reason) {
        afterCommit(() -> counter("saga_cancelled", reason).increment());
    }

    public void needsAttention(String reason) {
        afterCommit(() -> counter("saga_needs_attention", reason).increment());
    }

    private Counter counter(String name, String reason) {
        return Counter.builder(name).tag("reason", reason).register(registry);
    }

    private static void afterCommit(Runnable action) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            action.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }
}
