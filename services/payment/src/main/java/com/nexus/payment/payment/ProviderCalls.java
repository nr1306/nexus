package com.nexus.payment.payment;

import com.nexus.payment.gateway.PaymentGateway.AuthorizationResult;
import com.nexus.payment.gateway.PaymentGateway.OperationResult;
import com.nexus.payment.payment.PaymentRecord.Operation;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Makes a provider call and records it in {@code payment_attempts} (ADR 0010), plus the metrics
 * {@code payment_provider_calls_total{provider,operation,outcome}} and
 * {@code payment_provider_call_seconds{provider,operation}}.
 *
 * <p>The row is written in its own transaction (REQUIRES_NEW, a second pooled connection for a moment),
 * so it survives a rollback of the caller's transaction. Failing to write it never fails the payment.
 */
@Component
public class ProviderCalls {

    private static final Logger log = LoggerFactory.getLogger(ProviderCalls.class);
    private static final int MAX_REASON = 500;

    private record Outcome(String outcome, String reason) {
    }

    private final JdbcTemplate jdbc;
    private final TransactionTemplate ownTransaction;
    private final MeterRegistry meterRegistry;
    private final Clock clock;

    public ProviderCalls(JdbcTemplate jdbc, PlatformTransactionManager transactionManager, MeterRegistry meterRegistry,
                         Clock clock) {
        this.jdbc = jdbc;
        this.ownTransaction = new TransactionTemplate(transactionManager);
        this.ownTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.meterRegistry = meterRegistry;
        this.clock = clock;
    }

    public AuthorizationResult authorize(UUID orderId, String provider, Supplier<AuthorizationResult> call) {
        return call(orderId, Operation.AUTHORIZE, provider, call,
                r -> new Outcome(r.approved() ? "SUCCEEDED" : "DECLINED", r.declineReason()));
    }

    public OperationResult operation(UUID orderId, Operation operation, String provider, Supplier<OperationResult> call) {
        return call(orderId, operation, provider, call,
                r -> new Outcome(r.succeeded() ? "SUCCEEDED" : "FAILED", r.failureReason()));
    }

    private <T> T call(UUID orderId, Operation operation, String provider, Supplier<T> call, Function<T, Outcome> classify) {
        Timer timer = meterRegistry.timer("payment_provider_call_seconds", "provider", provider, "operation", operation.name());
        Instant started = clock.instant();
        Timer.Sample sample = Timer.start(meterRegistry);
        T result;
        try {
            result = call.get();
        } catch (RuntimeException e) {
            sample.stop(timer);
            record(orderId, operation, provider, new Outcome("ERROR", e.getClass().getSimpleName() + ": " + e.getMessage()), started);
            throw e;
        }
        sample.stop(timer);
        record(orderId, operation, provider, classify.apply(result), started);
        return result;
    }

    private void record(UUID orderId, Operation operation, String provider, Outcome outcome, Instant started) {
        meterRegistry.counter("payment_provider_calls_total", "provider", provider, "operation", operation.name(),
                "outcome", outcome.outcome()).increment();
        String reason = outcome.reason() == null ? null : outcome.reason().substring(0, Math.min(outcome.reason().length(), MAX_REASON));
        long millis = Duration.between(started, clock.instant()).toMillis();
        try {
            ownTransaction.executeWithoutResult(status -> jdbc.update("""
                            INSERT INTO payment_attempts (order_id, operation, provider, idempotency_key, outcome,
                                                          failure_reason, duration_ms, attempted_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                    orderId, operation.name(), provider, PaymentRecord.idempotencyKey(orderId, operation),
                    outcome.outcome(), reason, (int) Math.min(Integer.MAX_VALUE, millis), Timestamp.from(started)));
        } catch (RuntimeException e) {
            log.warn("Could not record payment attempt {} {}: {}", operation, outcome.outcome(), e.toString());
        }
    }
}
