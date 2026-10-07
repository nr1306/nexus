package com.nexus.payment.gateway;

/**
 * Port to the payment provider. Every call carries an idempotency key ({@code <orderId>:<operation>});
 * the provider must return the original result for a repeated key instead of acting twice
 * (ADR 0002). Transient failures (timeouts, 5xx) are thrown; business outcomes are returned.
 */
public interface PaymentGateway {

    String name();

    AuthorizationResult authorize(String idempotencyKey, long amountCents, String currency, String paymentMethod);

    OperationResult capture(String idempotencyKey, String authorizationId, long amountCents, String currency);

    OperationResult voidAuthorization(String idempotencyKey, String authorizationId);

    record AuthorizationResult(boolean approved, String authorizationId, String declineReason) {

        public static AuthorizationResult approved(String authorizationId) {
            return new AuthorizationResult(true, authorizationId, null);
        }

        public static AuthorizationResult declined(String reason) {
            return new AuthorizationResult(false, null, reason);
        }
    }

    record OperationResult(boolean succeeded, String failureReason) {

        public static OperationResult success() {
            return new OperationResult(true, null);
        }

        public static OperationResult failed(String reason) {
            return new OperationResult(false, reason);
        }
    }
}
