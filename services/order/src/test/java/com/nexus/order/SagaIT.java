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
 * Drives sagas through every path by feeding Inventory/Payment/Fulfillment replies to the reply handler.
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
        assertThat(lastCommand(orderId).eventType()).isEqualTo("AuthorizePayment");

        assertThat(fraudChecks.runPending()).isEqualTo(1);
        assertThat(state(orderId)).isEqualTo("FRAUD_APPROVED");

        replies.handle(replyTo(lastCommand(orderId), "PaymentCaptured"));
        assertThat(state(orderId)).isEqualTo("FULFILLING");
        EventEnvelope ship = lastCommand(orderId);
        assertThat(ship.eventType()).isEqualTo("CreateShipment");
        assertThat(ship.payload().get("customerId").asText()).isEqualTo("customer-1");
        assertThat(ship.payload().get("items")).hasSize(2);

        replies.handle(replyTo(ship, "ShipmentCreated", objectMapper.createObjectNode().put("shipmentId", UUID.randomUUID().toString())));

        assertThat(state(orderId)).isEqualTo("COMPLETED");
        assertThat(commands(orderId)).containsExactly(
                "ReserveInventory", "AuthorizePayment", "CapturePayment", "CreateShipment", "CommitInventory");
        assertThat(orderEvents(orderId)).containsExactly("OrderCreated", "OrderCompleted");
        assertThat(orderEvent(orderId, "OrderCompleted").payload().get("customerId").asText()).isEqualTo("customer-1");
        assertThat(counter("saga_completed")).isEqualTo(completedBefore + 1);

        // Fulfillment dispatches after OrderCompleted; its ShipmentShipped event changes nothing.
        replies.handle(replyTo(ship, "ShipmentShipped"));
        assertThat(state(orderId)).isEqualTo("COMPLETED");
    }

    @Test
    void fulfillmentFailureRefundsVoidsReleasesThenCancels() {
        UUID orderId = placeOrderAndCapture();

        replies.handle(failureReplyTo(lastCommand(orderId), "FulfillmentFailed", "RESTRICTED_SKU"));
        assertThat(state(orderId)).isEqualTo("COMPENSATING");
        assertThat(lastCommand(orderId).eventType()).isEqualTo("RefundPayment");
        replies.handle(replyTo(lastCommand(orderId), "PaymentRefunded", objectMapper.createObjectNode().put("refunded", true)));
        assertThat(lastCommand(orderId).eventType()).isEqualTo("VoidPayment");
        replies.handle(replyTo(lastCommand(orderId), "PaymentVoided", objectMapper.createObjectNode().put("voided", false)));
        assertThat(lastCommand(orderId).eventType()).isEqualTo("ReleaseInventory");
        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("RESTRICTED_SKU");
        assertThat(commands(orderId)).containsExactly("ReserveInventory", "AuthorizePayment", "CapturePayment",
                "CreateShipment", "RefundPayment", "VoidPayment", "ReleaseInventory");
        assertThat(orderEvents(orderId)).containsExactly("OrderCreated", "OrderCancelled");
        assertThat(orderEvent(orderId, "OrderCancelled").payload().get("customerId").asText()).isEqualTo("customer-1");
    }

    @Test
    void shipmentTimeoutCancelsShipmentThenRefundsVoidsReleases() {
        UUID orderId = placeOrderAndCapture();

        expireCurrentStep(orderId);
        sweeper.sweep();
        assertThat(commands(orderId).stream().filter("CreateShipment"::equals)).hasSize(2);
        expireCurrentStep(orderId);
        sweeper.sweep();

        assertThat(lastCommand(orderId).eventType()).isEqualTo("CancelShipment");
        replies.handle(replyTo(lastCommand(orderId), "ShipmentCancelled", objectMapper.createObjectNode().put("cancelled", true)));
        replies.handle(replyTo(lastCommand(orderId), "PaymentRefunded", objectMapper.createObjectNode().put("refunded", true)));
        replies.handle(replyTo(lastCommand(orderId), "PaymentVoided", objectMapper.createObjectNode().put("voided", false)));
        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("TIMEOUT_CREATE_SHIPMENT");
        assertThat(commands(orderId).subList(5, 9))
                .containsExactly("CancelShipment", "RefundPayment", "VoidPayment", "ReleaseInventory");
    }

    @Test
    void captureTimeoutRefundsThenVoidsThenReleases() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));
        fraudChecks.runPending();

        expireCurrentStep(orderId);
        sweeper.sweep();
        expireCurrentStep(orderId);
        sweeper.sweep();
        // Whether or not the capture happened, refund + void leave no money taken and no hold.
        replies.handle(replyTo(lastCommand(orderId), "PaymentRefunded", objectMapper.createObjectNode().put("refunded", false)));
        replies.handle(replyTo(lastCommand(orderId), "PaymentVoided", objectMapper.createObjectNode().put("voided", true)));
        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("TIMEOUT_CAPTURE_PAYMENT");
        assertThat(commands(orderId)).containsExactly("ReserveInventory", "AuthorizePayment", "CapturePayment",
                "CapturePayment", "RefundPayment", "VoidPayment", "ReleaseInventory");
    }

    @Test
    void failedRefundMarksSagaNeedsAttention() {
        UUID orderId = placeOrderAndCapture();
        replies.handle(failureReplyTo(lastCommand(orderId), "FulfillmentFailed", "RESTRICTED_SKU"));

        replies.handle(failureReplyTo(lastCommand(orderId), "PaymentRefundFailed", "REFUND_DECLINED"));

        assertThat(state(orderId)).isEqualTo("NEEDS_ATTENTION");
        assertThat(failureReason(orderId)).isEqualTo("REFUND_PAYMENT_FAILED_REFUND_DECLINED");
        assertThat(commands(orderId).getLast()).isEqualTo("RefundPayment");
    }

    @Test
    void reservationExpiredMidSagaCompensatesIncludingInFlightStep() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));
        fraudChecks.runPending();

        replies.handle(replyTo(lastCommand(orderId), "ReservationExpired"));

        assertThat(state(orderId)).isEqualTo("COMPENSATING");
        assertThat(failureReason(orderId)).isEqualTo("RESERVATION_EXPIRED");
        // Awaiting capture: it may have happened, so refund, then void, then release (a no-op for Inventory).
        assertThat(commands(orderId).subList(3, 4)).containsExactly("RefundPayment");
        replies.handle(replyTo(lastCommand(orderId), "PaymentRefunded", objectMapper.createObjectNode().put("refunded", false)));
        replies.handle(replyTo(lastCommand(orderId), "PaymentVoided", objectMapper.createObjectNode().put("voided", true)));
        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));
        assertThat(state(orderId)).isEqualTo("CANCELLED");
    }

    @Test
    void reservationExpiredAfterCompletionOrWhileCompensatingIsIgnored() {
        UUID completed = placeOrderAndCapture();
        replies.handle(replyTo(lastCommand(completed), "ShipmentCreated"));
        UUID compensating = placeOrder();
        replies.handle(replyTo(lastCommand(compensating), "InventoryReserved"));
        replies.handle(failureReplyTo(lastCommand(compensating), "PaymentDeclined", "CARD_DECLINED"));

        replies.handle(replyTo(lastCommand(completed), "ReservationExpired"));
        replies.handle(replyTo(lastCommand(compensating), "ReservationExpired"));

        assertThat(state(completed)).isEqualTo("COMPLETED");
        assertThat(state(compensating)).isEqualTo("COMPENSATING");
        assertThat(failureReason(compensating)).isEqualTo("CARD_DECLINED");
        assertThat(commands(compensating).getLast()).isEqualTo("ReleaseInventory");
        assertThat(commands(compensating).stream().filter("ReleaseInventory"::equals)).hasSize(1);
    }

    private EventEnvelope orderEvent(UUID orderId, String type) {
        return outbox(orderId).stream().map(Written::envelope).filter(e -> e.eventType().equals(type)).findFirst().orElseThrow();
    }

    private UUID placeOrderAndCapture() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));
        fraudChecks.runPending();
        replies.handle(replyTo(lastCommand(orderId), "PaymentCaptured"));
        assertThat(state(orderId)).isEqualTo("FULFILLING");
        return orderId;
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
        fraudChecks.runPending();

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
        fraudChecks.runPending();
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
    void fraudRejectionVoidsPaymentAndReleasesInventory() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));
        fraud.set(StubFraudClient.Mode.REJECT);

        fraudChecks.runPending();
        assertThat(state(orderId)).isEqualTo("COMPENSATING");
        replies.handle(replyTo(lastCommand(orderId), "PaymentVoided", objectMapper.createObjectNode().put("voided", true)));
        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));

        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("FRAUD_REJECTED");
        assertThat(commands(orderId)).containsExactly(
                "ReserveInventory", "AuthorizePayment", "VoidPayment", "ReleaseInventory");
    }

    @Test
    void fraudOutageRetriesAndCompletesIfFraudRecoversBeforeDeadline() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));
        fraud.set(StubFraudClient.Mode.UNAVAILABLE);

        assertThat(fraudChecks.runPending()).isZero();
        assertThat(fraudChecks.runPending()).isZero();
        assertThat(state(orderId)).isEqualTo("PAYMENT_AUTHORIZED");

        fraud.set(StubFraudClient.Mode.APPROVE);
        fraudChecks.runPending();

        assertThat(state(orderId)).isEqualTo("FRAUD_APPROVED");
        assertThat(lastCommand(orderId).eventType()).isEqualTo("CapturePayment");
    }

    @Test
    void fraudOutagePastDeadlineVoidsAndReleases() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));
        fraud.set(StubFraudClient.Mode.UNAVAILABLE);
        fraudChecks.runPending();

        expireCurrentStep(orderId);
        sweeper.sweep();

        assertThat(state(orderId)).isEqualTo("COMPENSATING");
        assertThat(lastCommand(orderId).eventType()).isEqualTo("VoidPayment");
        replies.handle(replyTo(lastCommand(orderId), "PaymentVoided", objectMapper.createObjectNode().put("voided", true)));
        replies.handle(replyTo(lastCommand(orderId), "InventoryReleased"));
        assertThat(state(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("TIMEOUT_FRAUD_CHECK");
    }

    @Test
    void lateVerdictAfterCompensationStartedIsIgnored() {
        UUID orderId = placeOrder();
        replies.handle(replyTo(lastCommand(orderId), "InventoryReserved"));
        replies.handle(replyTo(lastCommand(orderId), "PaymentAuthorized"));
        expireCurrentStep(orderId);
        sweeper.sweep();

        fraudChecks.check(orderId);

        assertThat(state(orderId)).isEqualTo("COMPENSATING");
        assertThat(commands(orderId).stream().filter("CapturePayment"::equals)).isEmpty();
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
