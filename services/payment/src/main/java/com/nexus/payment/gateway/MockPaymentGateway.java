package com.nexus.payment.gateway;

import com.nexus.payment.config.PaymentProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deterministic, stateless provider for local runs and load tests. Behaviour is chosen by the payment
 * method, using Stripe's test token names where Stripe has one:
 * <ul>
 *   <li>{@code pm_card_chargeDeclined} → authorization declined (CARD_DECLINED)</li>
 *   <li>{@code pm_card_chargeDeclinedInsufficientFunds} → declined (INSUFFICIENT_FUNDS)</li>
 *   <li>{@code pm_mock_captureFails} → authorized, but capture fails (mock only)</li>
 *   <li>{@code pm_mock_refundFails} → authorized and captured, but the refund is refused (mock only)</li>
 *   <li>anything else → approved</li>
 * </ul>
 * Results are derived from the idempotency key, so a repeated key returns the same result even after
 * a restart, like a real provider.
 */
@Component
@ConditionalOnProperty(name = "nexus.payment.gateway", havingValue = "mock", matchIfMissing = true)
public class MockPaymentGateway implements PaymentGateway {

    public static final String DECLINED = "pm_card_chargeDeclined";
    public static final String INSUFFICIENT_FUNDS = "pm_card_chargeDeclinedInsufficientFunds";
    public static final String CAPTURE_FAILS = "pm_mock_captureFails";
    public static final String REFUND_FAILS = "pm_mock_refundFails";

    private static final String AUTH_PREFIX = "mock_auth_";
    private static final String NO_CAPTURE_PREFIX = "mock_auth_nocap_";
    private static final String NO_REFUND_PREFIX = "mock_auth_noref_";

    private final Duration latency;

    /** Provider calls per idempotency key; a test hook for "acted at most once". */
    private final ConcurrentHashMap<String, Integer> callsByKey = new ConcurrentHashMap<>();

    public MockPaymentGateway(PaymentProperties properties) {
        this.latency = properties.mockLatency();
    }

    @Override
    public String name() {
        return "mock";
    }

    @Override
    public AuthorizationResult authorize(String idempotencyKey, long amountCents, String currency, String paymentMethod) {
        record(idempotencyKey);
        return switch (paymentMethod) {
            case DECLINED -> AuthorizationResult.declined("CARD_DECLINED");
            case INSUFFICIENT_FUNDS -> AuthorizationResult.declined("INSUFFICIENT_FUNDS");
            case CAPTURE_FAILS -> AuthorizationResult.approved(NO_CAPTURE_PREFIX + stableId(idempotencyKey));
            case REFUND_FAILS -> AuthorizationResult.approved(NO_REFUND_PREFIX + stableId(idempotencyKey));
            default -> AuthorizationResult.approved(AUTH_PREFIX + stableId(idempotencyKey));
        };
    }

    @Override
    public OperationResult capture(String idempotencyKey, String authorizationId, long amountCents, String currency) {
        record(idempotencyKey);
        return authorizationId.startsWith(NO_CAPTURE_PREFIX)
                ? OperationResult.failed("CAPTURE_DECLINED")
                : OperationResult.success();
    }

    @Override
    public OperationResult voidAuthorization(String idempotencyKey, String authorizationId) {
        record(idempotencyKey);
        return OperationResult.success();
    }

    @Override
    public OperationResult refund(String idempotencyKey, String authorizationId, long amountCents, String currency) {
        record(idempotencyKey);
        return authorizationId.startsWith(NO_REFUND_PREFIX)
                ? OperationResult.failed("REFUND_DECLINED")
                : OperationResult.success();
    }

    /** Number of provider calls made with this idempotency key (test hook). */
    public int calls(String idempotencyKey) {
        return callsByKey.getOrDefault(idempotencyKey, 0);
    }

    private void record(String idempotencyKey) {
        callsByKey.merge(idempotencyKey, 1, Integer::sum);
        if (!latency.isZero()) {
            try {
                Thread.sleep(latency);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted during mock provider call", e);
            }
        }
    }

    private static UUID stableId(String idempotencyKey) {
        return UUID.nameUUIDFromBytes(idempotencyKey.getBytes(StandardCharsets.UTF_8));
    }
}
