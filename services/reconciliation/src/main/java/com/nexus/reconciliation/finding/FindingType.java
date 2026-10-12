package com.nexus.reconciliation.finding;

import com.nexus.reconciliation.messaging.ReconciliationTopics;

/**
 * The invariants Reconciliation checks (SPEC §7, ADR 0009). {@link #violation} is an SQL condition
 * over {@code order_projection p}; {@code :stuckBefore} is bound for {@link #SAGA_STUCK}.
 * A type with a repair command gets that command sent when it is detected.
 */
public enum FindingType {

    /** Invariant 1. Flag only: a capture can't be re-run without a live authorization. */
    COMPLETED_NOT_CAPTURED("p.completed_at IS NOT NULL AND p.captured_at IS NULL", null, null),

    /** Invariant 1. Flag only: Fulfillment dispatches on OrderCompleted, so a late CreateShipment never ships. */
    COMPLETED_NOT_SHIPPED("p.completed_at IS NOT NULL AND p.shipment_created_at IS NULL", null, null),

    /** Invariant 2. */
    CANCELLED_BUT_CHARGED("p.cancelled_at IS NOT NULL AND p.captured_at IS NOT NULL AND p.refunded_at IS NULL",
            "RefundPayment", ReconciliationTopics.PAYMENT_COMMANDS),

    /**
     * Invariant 3 for a completed order: the stock was sold, so the hold is committed, never released
     * (releasing would put sold units back on sale).
     */
    STOCK_HELD_AFTER_COMPLETION("p.completed_at IS NOT NULL AND " + Sql.STOCK_HELD,
            "CommitInventory", ReconciliationTopics.INVENTORY_COMMANDS),

    /** Invariant 3 for a cancelled order: the hold goes back on sale. */
    STOCK_HELD_AFTER_CANCELLATION("p.cancelled_at IS NOT NULL AND " + Sql.STOCK_HELD,
            "ReleaseInventory", ReconciliationTopics.INVENTORY_COMMANDS),

    /** Invariant 4. Flag only until Order has a command topic to resume or compensate. */
    SAGA_STUCK("p.created_at IS NOT NULL AND p.completed_at IS NULL AND p.cancelled_at IS NULL "
            + "AND p.needs_attention_at IS NULL AND p.last_event_at < :stuckBefore", null, null),

    /** Invariant 4: a compensation exhausted its retries (SPEC §4); an operator must act. */
    NEEDS_ATTENTION("p.needs_attention_at IS NOT NULL AND p.completed_at IS NULL AND p.cancelled_at IS NULL", null, null);

    private static final class Sql {
        /** Stock is held: the last reserve is newer than every release, commit and expiry. */
        static final String STOCK_HELD = "p.reserved_at > "
                + "coalesce(greatest(p.released_at, p.committed_at, p.expired_at), '-infinity'::timestamptz)";
    }

    final String violation;
    final String repairCommand;
    final String repairTopic;

    FindingType(String violation, String repairCommand, String repairTopic) {
        this.violation = violation;
        this.repairCommand = repairCommand;
        this.repairTopic = repairTopic;
    }

    public boolean repairable() {
        return repairCommand != null;
    }
}
