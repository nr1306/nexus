package com.nexus.order.saga;

import com.nexus.order.config.OrderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.UUID;

/**
 * Finds sagas whose awaited reply is overdue and hands each to {@link SagaOrchestrator#onTimeout}
 * in its own transaction. Safe to run on several pods: rows are claimed with SKIP LOCKED.
 */
@Component
public class SagaTimeoutSweeper {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutSweeper.class);

    private final SagaRepository sagas;
    private final SagaOrchestrator orchestrator;
    private final TransactionTemplate transactionTemplate;
    private final OrderProperties properties;
    private final Clock clock;

    public SagaTimeoutSweeper(SagaRepository sagas, SagaOrchestrator orchestrator, TransactionTemplate transactionTemplate,
                              OrderProperties properties, Clock clock) {
        this.sagas = sagas;
        this.orchestrator = orchestrator;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    /** @return number of overdue sagas handled */
    public int sweep() {
        int handled = 0;
        for (UUID orderId : sagas.findOverdue(clock.instant(), properties.sweepBatchSize())) {
            try (var ignored = MDC.putCloseable("orderId", orderId.toString())) {
                Boolean done = transactionTemplate.execute(status -> sagas.lockIfOverdue(orderId, clock.instant())
                        .map(saga -> {
                            try (var sagaId = MDC.putCloseable("sagaId", saga.sagaId().toString())) {
                                orchestrator.onTimeout(saga);
                            }
                            return true;
                        })
                        .orElse(false));
                if (Boolean.TRUE.equals(done)) {
                    handled++;
                }
            } catch (RuntimeException e) {
                log.error("Failed to handle saga timeout", e);
            }
        }
        return handled;
    }
}
