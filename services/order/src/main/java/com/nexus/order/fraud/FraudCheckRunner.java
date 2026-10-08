package com.nexus.order.fraud;

import com.nexus.order.order.OrderDetails;
import com.nexus.order.order.OrderRepository;
import com.nexus.order.saga.SagaInstance;
import com.nexus.order.saga.SagaOrchestrator;
import com.nexus.order.saga.SagaRepository;
import com.nexus.order.saga.SagaStep;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;

/**
 * Carries out the FRAUD_CHECK saga step (ADR 0005). The gRPC call happens outside any transaction, so no
 * saga row lock or DB connection is held during it; the verdict is then applied in a short transaction
 * that re-checks the saga is still awaiting this step.
 *
 * <p>Triggered right after the transaction that entered FRAUD_CHECK commits, and retried by
 * {@link #runPending()} (polled) while Fraud is unavailable. If no verdict arrives before the step
 * deadline, the timeout sweeper compensates. Fraud's Evaluate is idempotent per order, so duplicate
 * calls (e.g. from several pods) are harmless.
 */
@Component
public class FraudCheckRunner {

    private static final Logger log = LoggerFactory.getLogger(FraudCheckRunner.class);
    private static final int BATCH = 100;

    private final FraudClient fraudClient;
    private final SagaRepository sagas;
    private final OrderRepository orders;
    private final SagaOrchestrator orchestrator;
    private final TransactionTemplate transactionTemplate;
    private final ThreadPoolTaskExecutor fraudCheckExecutor;
    private final FraudClientProperties properties;
    private final MeterRegistry meterRegistry;

    public FraudCheckRunner(FraudClient fraudClient, SagaRepository sagas, OrderRepository orders,
                            SagaOrchestrator orchestrator, TransactionTemplate transactionTemplate,
                            ThreadPoolTaskExecutor fraudCheckExecutor, FraudClientProperties properties,
                            MeterRegistry meterRegistry) {
        this.fraudClient = fraudClient;
        this.sagas = sagas;
        this.orders = orders;
        this.orchestrator = orchestrator;
        this.transactionTemplate = transactionTemplate;
        this.fraudCheckExecutor = fraudCheckExecutor;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    void onRequested(FraudCheckRequested event) {
        if (properties.autoRun()) {
            fraudCheckExecutor.execute(() -> check(event.orderId()));
        }
    }

    /** Checks every saga currently awaiting a fraud verdict. @return verdicts applied */
    public int runPending() {
        int applied = 0;
        for (UUID orderId : sagas.findAwaiting(SagaStep.FRAUD_CHECK, BATCH)) {
            if (check(orderId)) {
                applied++;
            }
        }
        return applied;
    }

    /** @return {@code true} if a verdict was obtained and applied */
    public boolean check(UUID orderId) {
        try (var o = MDC.putCloseable("orderId", orderId.toString())) {
            Optional<SagaInstance> awaiting = sagas.findByOrderId(orderId).filter(s -> s.step() == SagaStep.FRAUD_CHECK);
            if (awaiting.isEmpty()) {
                return false;
            }
            UUID sagaId = awaiting.get().sagaId();
            OrderDetails order = orders.findDetails(orderId).orElseThrow();

            FraudClient.Verdict verdict;
            try (var s = MDC.putCloseable("sagaId", sagaId.toString())) {
                verdict = fraudClient.evaluate(order, sagaId);
            } catch (FraudUnavailableException e) {
                meterRegistry.counter("fraud_checks", "outcome", "unavailable").increment();
                log.warn("Fraud check unavailable: {}", e.getMessage());
                return false;
            }
            meterRegistry.counter("fraud_checks", "outcome", verdict.approved() ? "approve" : "reject").increment();

            return Boolean.TRUE.equals(transactionTemplate.execute(status -> sagas.lockByOrderId(orderId)
                    .filter(s -> s.sagaId().equals(sagaId) && s.step() == SagaStep.FRAUD_CHECK)
                    .map(s -> {
                        orchestrator.onFraudVerdict(s, verdict);
                        return true;
                    })
                    .orElse(false)));
        } catch (RuntimeException e) {
            log.error("Fraud check failed", e);
            return false;
        }
    }
}
