package com.nexus.order;

import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.order.messaging.SagaReplyHandler;
import com.nexus.order.saga.SagaTimeoutSweeper;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives sagas through every path by feeding Inventory/Payment replies to the reply handler.
 * Every saga step gets a failure test asserting the compensations and the final state (CLAUDE.md).
 */
class SagaIT extends OrderIntegrationTest {

    @Autowired
    SagaReplyHandler replies;

    @Autowired
    SagaTimeoutSweeper sweeper;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    void happyPathReservesAuthorizesCapturesAndCompletes() {
        double completedBefore = counter("saga_completed");
        UUID orderId = placeOrder();
        assertThat(state(orderId)).isEqualTo("PENDING");
        assertThat(lastCommand(orderId).payload().get("items")).hasSize(2);

        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        assertThat(state(orderId)).isEqualTo("INVENTORY_RESERVED");
        EventEnvelope authorize = lastCommand(orderId);
        assertThat(authorize.eventType()).isEqualTo("AuthorizePayment");
        assertThat(authorize.payload().get("amountCents").asLong()).isEqualTo(5_000);
        assertThat(authorize.payload().get("currency").asText()).isEqualTo("USD");
        assertThat(authorize.payload().get("paymentMethod").asText()).isEqualTo("pm_card_visa");

        replies.handle(replyTo(authorize, "PaymentAuthorized"));
        assertThat(state(orderId)).isEqualTo("PAYMENT_AUTHORIZED");

        replies.handle(replyTo(lastCommand(orderId), "PaymentCaptured"));

        assertThat(state(orderId)).isEqualTo("COMPLETED");
        assertThat(commands(orderId)).containsExactly("ReserveInventory", "AuthorizePayment", "CapturePayment", "CommitInventory");
        assertThat(orderEvents(orderId)).containsExactly("OrderCreated", "OrderCompleted");
        assertThat(counter("saga_completed")).isEqualTo(completedBefore + 1);
    }

    @Test
    void outOfStockCancelsWithoutCompensation() {
        UUID orderId = placeOrder();

        replies.handle(failureReplyTo(lastCommand(orderId), "InventoryRejected", "OUT_OF_STOCK"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("OUT_OF_STOCK");
        assertThat(commands(orderId)).containsExactly("ReserveInventory");
        assertThat(orderEvents(orderId)).containsExactly("OrderCreated", "OrderCancelled");
    }

    @Test
    void cardDeclinedReleasesInventoryThenCancels() {
        double compensatedBefore = counter("saga_compensated", "CARD_DECLINED");
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));

        replies.handle(failureReplyTo(lastCommand(orderId), "PaymentDeclined", "CARD_DECLINED"));
        assertThat(state(orderId)).isEqualTo("COMPENSATING");
        EventEnvelope release = lastCommand(orderId);
        assertThat(release.eventType()).isEqualTo("ReleaseInventory");
        assertThat(release.payload().get("reason").asText()).isEqualTo("CARD_DECLINED");

        replies.handle(replyTo(release, "InventoryReleased"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("CARD_DECLINED");
        assertThat(commands(orderId)).containsExactly("ReserveInventory", "AuthorizePayment", "ReleaseInventory");
        assertThat(orderEvents(orderId)).containsExactly("OrderCreated", "OrderCancelled");
        assertThat(counter("saga_compensated", "CARD_DECLINED")).isEqualTo(compensatedBefore + 1);
    }

    @Test
    void captureFailureVoidsPaymentThenReleasesInventoryThenCancels() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));

        replies.handle(failureReplyTo(lastCommand(orderId), "CaptureFailed", "CAPTURE_DECLINED"));
        assertThat(lastCommand(orderId).eventType()).isEqualTo("VoidPayment");
        replies.handle(replyTo(lastCommand(orderId), "PaymentVoided",
                objectMapper.createObjectNode().put("voided", true)));
        assertThat(lastCommand(orderId).eventType()).isEqualTo("ReleaseInventory");
        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("CAPTURE_DECLINED");
        assertThat(commands(orderId)).containsExactly(
                "ReserveInventory", "AuthorizePayment", "CapturePayment", "VoidPayment", "ReleaseInventory");
    }

    @Test
    void failedVoidMarksSagaNeedsAttention() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));
        replies.handle(failureReplyTo(lastCommand(orderId), "CaptureFailed", "CAPTURE_DECLINED"));

        replies.handle(failureReplyTo(lastCommand(orderId), "PaymentVoidFailed", "ALREADY_CAPTURED"));

        assertThat(state(orderId)).isEqualTo("NEEDS_ATTENTION");
        assertThat(failureReason(orderId)).isEqualTo("VOID_PAYMENT_FAILED_ALREADY_CAPTURED");
        assertThat(orderEvents(orderId)).containsExactly("OrderCreated", "OrderNeedsAttention");
    }

    @Test
    void duplicateReplyIsAppliedOnce() {
        UUID orderId = placeOrder();
        EventEnvelope reserved = replyTo(lastCommand(orderId), "InventoryReserved");

        assertThat(replies.handle(reserved)).isTrue();
        assertThat(replies.handle(reserved)).isFalse();

        assertThat(commands(orderId)).containsExactly("ReserveInventory", "AuthorizePayment");
    }

    @Test
    void staleReplyFromRetriedCommandIsIgnored() {
        UUID orderId = placeOrder();
        EventEnvelope reserve = lastCommand(orderId);
        replies.handle(replyTo(reserve, "InventoryReserved"));

        // A second InventoryReserved (new eventId), e.g. the reply to a re-sent ReserveInventory.
        replies.handle(replyTo(reserve, "InventoryReserved"));

        assertThat(state(orderId)).isEqualTo("INVENTORY_RESERVED");
        assertThat(commands(orderId)).containsExactly("ReserveInventory", "AuthorizePayment");
    }

    @Test
    void replyForAnotherSagaIsIgnored() {
        UUID orderId = placeOrder();
        EventEnvelope reserve = lastCommand(orderId);
        EventEnvelope foreign = new EventEnvelope(UUID.randomUUID(), "InventoryReserved", 1, orderId, UUID.randomUUID(),
                reserve.occurredAt(), objectMapper.createObjectNode());

        replies.handle(foreign);

        assertThat(state(orderId)).isEqualTo("PENDING");
    }

    @Test
    void lateReplyAfterCancellationIsIgnored() {
        UUID orderId = placeOrder();
        EventEnvelope reserve = lastCommand(orderId);
        replies.handle(failureReplyTo(reserve, "InventoryRejected", "OUT_OF_STOCK"));

        replies.handle(replyTo(reserve, "InventoryReserved"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(commands(orderId)).containsExactly("ReserveInventory");
    }

    @Test
    void stepTimeoutRetriesOnceThenCompensatesIncludingInFlightStep() {
        UUID orderId = placeOrder();

        expireCurrentStep(orderId);
        sweeper.sweep();
        assertThat(commands(orderId)).containsExactly("ReserveInventory", "ReserveInventory");
        assertThat(state(orderId)).isEqualTo("PENDING");

        expireCurrentStep(orderId);
        sweeper.sweep();
        assertThat(state(orderId)).isEqualTo("COMPENSATING");
        assertThat(lastCommand(orderId).eventType()).isEqualTo("ReleaseInventory");

        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));
        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("TIMEOUT_RESERVE_INVENTORY");
    }

    @Test
    void authorizeTimeoutVoidsAndReleases() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));

        expireCurrentStep(orderId);
        sweeper.sweep();
        expireCurrentStep(orderId);
        sweeper.sweep();
        replies.handle(replyTo(lastCommand(orderId), "PaymentVoided", objectMapper.createObjectNode().put("voided", false)));
        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(commands(orderId)).containsExactly(
                "ReserveInventory", "AuthorizePayment", "AuthorizePayment", "VoidPayment", "ReleaseInventory");
    }

    @Test
    void compensationThatNeverAnswersEndsInNeedsAttention() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(failureReplyTo(lastCommand(orderId), "PaymentDeclined", "CARD_DECLINED"));

        for (int i = 0; i < 5; i++) {
            expireCurrentStep(orderId);
            sweeper.sweep();
        }

        assertThat(state(orderId)).isEqualTo("NEEDS_ATTENTION");
        assertThat(failureReason(orderId)).isEqualTo("COMPENSATION_TIMEOUT_RELEASE_INVENTORY");
        assertThat(commands(orderId).stream().filter("ReleaseInventory"::equals)).hasSize(5);
    }

    @Test
    void sweeperIgnoresSagasThatAreNotOverdue() {
        UUID orderId = placeOrder();

        sweeper.sweep();

        assertThat(commands(orderId)).containsExactly("ReserveInventory");
    }

    private double counter(String name, String... tags) {
        var search = meterRegistry.find(name);
        if (tags.length > 0) {
            search = search.tag("reason", tags[0]);
        }
        var counter = search.counter();
        return counter == null ? 0 : counter.count();
    }
}
