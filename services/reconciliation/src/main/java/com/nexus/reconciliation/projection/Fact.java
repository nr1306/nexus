package com.nexus.reconciliation.projection;

/** One timestamp column of {@code order_projection} (ADR 0009). */
enum Fact {
    CREATED("created_at"),
    COMPLETED("completed_at"),
    CANCELLED("cancelled_at"),
    NEEDS_ATTENTION("needs_attention_at"),
    RESERVED("reserved_at"),
    RELEASED("released_at"),
    COMMITTED("committed_at"),
    EXPIRED("expired_at"),
    AUTHORIZED("authorized_at"),
    CAPTURED("captured_at"),
    REFUNDED("refunded_at"),
    VOIDED("voided_at"),
    SHIPMENT_CREATED("shipment_created_at"),
    SHIPMENT_CANCELLED("shipment_cancelled_at"),
    SHIPPED("shipped_at");

    final String column;

    Fact(String column) {
        this.column = column;
    }
}
