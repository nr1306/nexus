package com.nexus.reconciliation;

import com.nexus.messaging.envelope.EventEnvelope;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Projection, invariant checks and repairs, driving the handler and checker directly (ADR 0009). */
class ReconciliationIT extends ReconciliationIntegrationTest {

    private static final String[] HAPPY_PATH = {"OrderCreated", "InventoryReserved", "PaymentAuthorized", "PaymentCaptured",
            "ShipmentCreated", "OrderCompleted", "InventoryCommitted", "ShipmentShipped"};

    @Test
    void duplicateEventIsAppliedOnce() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope created = event("OrderCreated", orderId, UUID.randomUUID());

        assertThat(handler.handle(created)).isTrue();
        assertThat(handler.handle(created)).isFalse();

        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Long.class,
                created.eventId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_projection WHERE order_id = ?", Long.class, orderId))
                .isEqualTo(1);
    }

    @Test
    void healthySagasInAnyArrivalOrderHaveNoFindings() {
        UUID completed = UUID.randomUUID();
        UUID refunded = UUID.randomUUID();
        UUID declined = UUID.randomUUID();
        // Events happen in saga order but arrive shuffled (four topics, retry topics): facts don't
        // depend on arrival order.
        UUID sagaId = UUID.randomUUID();
        List<EventEnvelope> happened = new ArrayList<>();
        for (String type : HAPPY_PATH) {
            clock.advance(Duration.ofMillis(1));
            happened.add(event(type, completed, sagaId));
        }
        Collections.shuffle(happened, new Random(7));
        happened.forEach(handler::handle);
        apply(refunded, UUID.randomUUID(), "OrderCreated", "InventoryReserved", "PaymentAuthorized", "PaymentCaptured",
                "PaymentRefunded", "PaymentVoided", "InventoryReleased", "OrderCancelled");
        apply(declined, UUID.randomUUID(), "OrderCreated", "InventoryReserved", "PaymentDeclined", "InventoryReleased",
                "OrderCancelled");
        clock.advance(PAST_SETTLE);

        checker.check();

        assertThat(findings(completed)).isEmpty();
        assertThat(findings(refunded)).isEmpty();
        assertThat(findings(declined)).isEmpty();
    }

    @Test
    void unsettledOrderIsNotCheckedYet() {
        UUID orderId = UUID.randomUUID();
        apply(orderId, UUID.randomUUID(), "OrderCreated", "PaymentCaptured", "OrderCancelled");
        clock.advance(Duration.ofSeconds(10));

        checker.check();
        assertThat(findings(orderId)).isEmpty();

        clock.advance(PAST_SETTLE);
        checker.check();
        assertThat(findings(orderId)).containsExactly("CANCELLED_BUT_CHARGED");
    }

    @Test
    void cancelledButChargedIsRefundedOnceAndResolvedByTheRefund() {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        apply(orderId, sagaId, "OrderCreated", "InventoryReserved", "PaymentAuthorized", "InventoryReleased",
                "OrderCancelled");
        // A capture published after the cancellation (the S6 injection for invariant 2).
        apply(orderId, sagaId, "PaymentCaptured");
        clock.advance(PAST_SETTLE);

        checker.check();
        checker.check();

        assertThat(openFindings(orderId)).containsExactly("CANCELLED_BUT_CHARGED");
        assertThat(repairs(orderId)).singleElement().satisfies(repair -> {
            assertThat(repair.eventType()).isEqualTo("RefundPayment");
            assertThat(repair.sagaId()).isEqualTo(sagaId);
            assertThat(repair.payload().get("reason").asText()).isEqualTo("RECONCILIATION_CANCELLED_BUT_CHARGED");
        });
        assertThat(jdbc.queryForObject("SELECT topic FROM outbox WHERE aggregate_id = ?", String.class, orderId.toString()))
                .isEqualTo("payment.commands");

        apply(orderId, sagaId, "PaymentRefunded");
        checker.check();

        assertThat(findings(orderId)).containsExactly("CANCELLED_BUT_CHARGED");
        assertThat(openFindings(orderId)).isEmpty();
    }

    @Test
    void stockReservedAgainAfterTheEndIsReleased() {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        apply(orderId, sagaId, "OrderCreated", "InventoryReserved", "PaymentDeclined", "InventoryReleased", "OrderCancelled",
                "InventoryReserved");
        clock.advance(PAST_SETTLE);

        checker.check();

        assertThat(openFindings(orderId)).containsExactly("STOCK_HELD_AFTER_CANCELLATION");
        assertThat(repairs(orderId)).extracting(EventEnvelope::eventType).containsExactly("ReleaseInventory");

        apply(orderId, sagaId, "InventoryReleased");
        checker.check();
        assertThat(openFindings(orderId)).isEmpty();
    }

    @Test
    void completedOrderStillHoldingStockIsCommittedNotReleased() {
        UUID held = UUID.randomUUID();
        UUID committed = UUID.randomUUID();
        apply(held, UUID.randomUUID(), "OrderCreated", "InventoryReserved", "PaymentAuthorized", "PaymentCaptured",
                "ShipmentCreated", "OrderCompleted");
        apply(committed, UUID.randomUUID(), HAPPY_PATH);
        clock.advance(PAST_SETTLE);

        checker.check();

        assertThat(findings(held)).containsExactly("STOCK_HELD_AFTER_COMPLETION");
        // The stock was sold: releasing it would put sold units back on sale.
        assertThat(repairs(held)).extracting(EventEnvelope::eventType).containsExactly("CommitInventory");
        assertThat(findings(committed)).isEmpty();
    }

    @Test
    void completedWithoutCaptureOrShipmentIsFlaggedWithoutRepair() {
        UUID orderId = UUID.randomUUID();
        apply(orderId, UUID.randomUUID(), "OrderCreated", "OrderCompleted");
        clock.advance(PAST_SETTLE);

        checker.check();

        assertThat(openFindings(orderId)).containsExactly("COMPLETED_NOT_CAPTURED", "COMPLETED_NOT_SHIPPED");
        assertThat(repairs(orderId)).isEmpty();
    }

    @Test
    void quietNonTerminalSagaIsStuckUntilItMovesAgain() {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        apply(orderId, sagaId, "OrderCreated");
        clock.advance(Duration.ofMinutes(4));
        checker.check();
        assertThat(findings(orderId)).isEmpty();

        clock.advance(Duration.ofMinutes(2));
        checker.check();
        assertThat(openFindings(orderId)).containsExactly("SAGA_STUCK");
        assertThat(repairs(orderId)).isEmpty();

        apply(orderId, sagaId, "InventoryReserved");
        checker.check();
        assertThat(openFindings(orderId)).isEmpty();
    }

    @Test
    void needsAttentionIsFlagged() {
        UUID orderId = UUID.randomUUID();
        apply(orderId, UUID.randomUUID(), "OrderCreated", "PaymentCaptured", "OrderNeedsAttention");
        clock.advance(PAST_SETTLE);

        checker.check();

        assertThat(openFindings(orderId)).containsExactly("NEEDS_ATTENTION");
    }

    @Test
    void repairIsResentWhileTheFindingStaysOpenUpToTheLimit() {
        UUID orderId = UUID.randomUUID();
        apply(orderId, UUID.randomUUID(), "OrderCreated", "PaymentCaptured", "OrderCancelled");
        clock.advance(PAST_SETTLE);
        checker.check();

        for (int i = 0; i < 5; i++) {
            clock.advance(Duration.ofMinutes(3));
            checker.check();
        }

        assertThat(repairs(orderId)).hasSize(3).extracting(EventEnvelope::eventType).containsOnly("RefundPayment");
        assertThat(jdbc.queryForObject("SELECT repair_attempts FROM findings WHERE order_id = ?", Integer.class, orderId))
                .isEqualTo(3);
        assertThat(openFindings(orderId)).containsExactly("CANCELLED_BUT_CHARGED");
    }
}
