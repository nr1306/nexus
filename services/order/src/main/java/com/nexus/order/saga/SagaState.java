package com.nexus.order.saga;

/**
 * Saga states (SPEC.md §4). While a forward step is awaited the state is the previous step's result,
 * except fulfillment: PAYMENT_CAPTURED becomes FULFILLING in the same transaction that sends
 * CreateShipment, so PAYMENT_CAPTURED is never stored (ADR 0006).
 */
public enum SagaState {
    PENDING,
    INVENTORY_RESERVED,
    PAYMENT_AUTHORIZED,
    FRAUD_APPROVED,
    PAYMENT_CAPTURED,
    FULFILLING,
    COMPLETED,
    COMPENSATING,
    CANCELLED,
    NEEDS_ATTENTION;

    public boolean isTerminal() {
        return this == COMPLETED || this == CANCELLED || this == NEEDS_ATTENTION;
    }
}
