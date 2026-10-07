package com.nexus.payment.payment;

import java.util.UUID;

/** One row of {@code payments}: the outcome of one operation for one order. */
public record PaymentRecord(
        UUID orderId,
        Operation operation,
        Status status,
        Long amountCents,
        String currency,
        String provider,
        String providerRef,
        String failureReason) {

    public enum Operation { AUTHORIZE, CAPTURE, VOID, REFUND }

    public enum Status { SUCCEEDED, FAILED, SKIPPED }

    public boolean succeeded() {
        return status == Status.SUCCEEDED;
    }

    /** Idempotency key sent to the provider: stable across redeliveries and retried commands. */
    public static String idempotencyKey(UUID orderId, Operation operation) {
        return orderId + ":" + operation;
    }
}
