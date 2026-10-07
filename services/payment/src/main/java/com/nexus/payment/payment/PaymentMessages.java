package com.nexus.payment.payment;

/**
 * Payloads of payment commands and events. Schemas: {@code contracts/events/payment/}.
 */
public final class PaymentMessages {

    public static final String AUTHORIZE_PAYMENT = "AuthorizePayment";
    public static final String CAPTURE_PAYMENT = "CapturePayment";
    public static final String VOID_PAYMENT = "VoidPayment";

    public static final String PAYMENT_AUTHORIZED = "PaymentAuthorized";
    public static final String PAYMENT_DECLINED = "PaymentDeclined";
    public static final String PAYMENT_CAPTURED = "PaymentCaptured";
    public static final String CAPTURE_FAILED = "CaptureFailed";
    public static final String PAYMENT_VOIDED = "PaymentVoided";
    public static final String PAYMENT_VOID_FAILED = "PaymentVoidFailed";

    /** Failure reasons recorded in {@code payments.failure_reason} and sent in replies. */
    public static final String INVALID_REQUEST = "INVALID_REQUEST";
    public static final String ORDER_CANCELLED = "ORDER_CANCELLED";
    public static final String NOT_AUTHORIZED = "NOT_AUTHORIZED";
    public static final String AUTHORIZATION_VOIDED = "AUTHORIZATION_VOIDED";
    public static final String ALREADY_CAPTURED = "ALREADY_CAPTURED";

    private PaymentMessages() {
    }

    public record AuthorizePayment(Long amountCents, String currency, String paymentMethod) {
    }

    public record PaymentAuthorized(long amountCents, String currency, String authorizationId) {
    }

    public record PaymentDeclined(String reason) {
    }

    public record PaymentCaptured(long amountCents, String currency) {
    }

    public record CaptureFailed(String reason) {
    }

    public record PaymentVoided(boolean voided) {
    }

    public record PaymentVoidFailed(String reason) {
    }
}
