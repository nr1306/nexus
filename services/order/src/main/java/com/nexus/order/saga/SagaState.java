package com.nexus.order.saga;

/** Saga states (SPEC.md §4). Phase 1 runs reserve → authorize → capture; fraud and fulfillment come in Phase 2. */
public enum SagaState {
    PENDING,
    INVENTORY_RESERVED,
    PAYMENT_AUTHORIZED,
    COMPLETED,
    COMPENSATING,
    CANCELLED,
    NEEDS_ATTENTION;

    public boolean isTerminal() {
        return this == COMPLETED || this == CANCELLED || this == NEEDS_ATTENTION;
    }
}
