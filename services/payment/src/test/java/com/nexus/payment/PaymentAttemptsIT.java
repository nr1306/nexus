package com.nexus.payment;

import com.nexus.payment.gateway.MockPaymentGateway;
import com.nexus.payment.gateway.PaymentGateway.OperationResult;
import com.nexus.payment.messaging.PaymentCommandHandler;
import com.nexus.payment.payment.PaymentRecord.Operation;
import com.nexus.payment.payment.ProviderCalls;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Every provider call is recorded in payment_attempts, even when its transaction rolls back (ADR 0010). */
class PaymentAttemptsIT extends PaymentIntegrationTest {

    @Autowired
    PaymentCommandHandler handler;

    @Autowired
    ProviderCalls providerCalls;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Test
    void eachProviderCallIsRecordedWithItsOutcome() {
        UUID approved = UUID.randomUUID();
        UUID declined = UUID.randomUUID();
        handler.handle(authorizeCommand(approved, 4999, "pm_card_visa"));
        handler.handle(captureCommand(approved));
        handler.handle(authorizeCommand(declined, 4999, MockPaymentGateway.DECLINED));
        // A retried command answers from the payments row: no second provider call.
        handler.handle(captureCommand(approved));

        assertThat(attempts(approved)).containsExactly(
                Map.of("operation", "AUTHORIZE", "outcome", "SUCCEEDED", "idempotency_key", approved + ":AUTHORIZE", "provider", "mock"),
                Map.of("operation", "CAPTURE", "outcome", "SUCCEEDED", "idempotency_key", approved + ":CAPTURE", "provider", "mock"));
        assertThat(attempts(declined)).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "DECLINED"));
        assertThat(jdbc.queryForObject("SELECT failure_reason FROM payment_attempts WHERE order_id = ?", String.class, declined))
                .isEqualTo("CARD_DECLINED");
    }

    @Test
    void failedCallIsRecordedAlthoughItsTransactionRollsBack() {
        UUID orderId = UUID.randomUUID();

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            jdbc.update("INSERT INTO payments (order_id, operation, status, provider) VALUES (?, 'VOID', 'SKIPPED', 'mock')", orderId);
            providerCalls.operation(orderId, Operation.VOID, "mock", () -> {
                throw new IllegalStateException("provider timed out");
            });
        })).hasMessage("provider timed out");

        assertThat(paymentRows(orderId, "VOID")).isZero();
        assertThat(attempts(orderId)).singleElement().satisfies(a -> assertThat(a).containsEntry("outcome", "ERROR"));
        assertThat(jdbc.queryForObject("SELECT failure_reason FROM payment_attempts WHERE order_id = ?", String.class, orderId))
                .isEqualTo("IllegalStateException: provider timed out");
    }

    @Test
    void failedOutcomeIsRecorded() {
        UUID orderId = UUID.randomUUID();
        transactionTemplate.executeWithoutResult(status ->
                providerCalls.operation(orderId, Operation.REFUND, "mock", () -> OperationResult.failed("REFUND_DECLINED")));

        assertThat(attempts(orderId)).singleElement().satisfies(a -> assertThat(a).containsEntry("outcome", "FAILED"));
    }

    private List<Map<String, Object>> attempts(UUID orderId) {
        return jdbc.queryForList("SELECT operation, outcome, idempotency_key, provider FROM payment_attempts "
                + "WHERE order_id = ? ORDER BY id", orderId);
    }
}
