package com.nexus.order.saga;

import com.nexus.order.messaging.OrderTopics;

import java.util.List;

/**
 * A command the saga sends and waits on, with the replies that end the wait.
 * Forward steps run in declaration order; compensation steps undo them (ADR 0003, 0005, 0006).
 */
public enum SagaStep {

    RESERVE_INVENTORY("ReserveInventory", OrderTopics.INVENTORY_COMMANDS, "InventoryReserved", "InventoryRejected", false),
    AUTHORIZE_PAYMENT("AuthorizePayment", OrderTopics.PAYMENT_COMMANDS, "PaymentAuthorized", "PaymentDeclined", false),
    /** Synchronous gRPC call to Fraud, not a Kafka command (ADR 0005); its result arrives via the fraud check runner. */
    FRAUD_CHECK(null, null, null, null, false),
    CAPTURE_PAYMENT("CapturePayment", OrderTopics.PAYMENT_COMMANDS, "PaymentCaptured", "CaptureFailed", false),
    CREATE_SHIPMENT("CreateShipment", OrderTopics.FULFILLMENT_COMMANDS, "ShipmentCreated", "FulfillmentFailed", false),

    CANCEL_SHIPMENT("CancelShipment", OrderTopics.FULFILLMENT_COMMANDS, "ShipmentCancelled", "ShipmentCancelFailed", true),
    REFUND_PAYMENT("RefundPayment", OrderTopics.PAYMENT_COMMANDS, "PaymentRefunded", "PaymentRefundFailed", true),
    VOID_PAYMENT("VoidPayment", OrderTopics.PAYMENT_COMMANDS, "PaymentVoided", "PaymentVoidFailed", true),
    RELEASE_INVENTORY("ReleaseInventory", OrderTopics.INVENTORY_COMMANDS, "InventoryReleased", null, true);

    /** Forward steps in execution order. */
    public static final List<SagaStep> FORWARD =
            List.of(RESERVE_INVENTORY, AUTHORIZE_PAYMENT, FRAUD_CHECK, CAPTURE_PAYMENT, CREATE_SHIPMENT);

    private final String commandType;
    private final String topic;
    private final String successReply;
    private final String failureReply;
    private final boolean compensation;

    SagaStep(String commandType, String topic, String successReply, String failureReply, boolean compensation) {
        this.commandType = commandType;
        this.topic = topic;
        this.successReply = successReply;
        this.failureReply = failureReply;
        this.compensation = compensation;
    }

    public String commandType() {
        return commandType;
    }

    public String topic() {
        return topic;
    }

    public boolean isSuccess(String replyType) {
        return successReply != null && successReply.equals(replyType);
    }

    public boolean isFailure(String replyType) {
        return failureReply != null && failureReply.equals(replyType);
    }

    public boolean isCompensation() {
        return compensation;
    }

    /** {@code true} for steps carried out by a direct call rather than a Kafka command and reply. */
    public boolean isSynchronous() {
        return commandType == null;
    }

    /** The next forward step, or {@code null} after the last one. */
    public SagaStep next() {
        int i = FORWARD.indexOf(this);
        return i >= 0 && i + 1 < FORWARD.size() ? FORWARD.get(i + 1) : null;
    }

    /** The compensation that undoes this forward step, or {@code null} if none exists yet. */
    public SagaStep undo() {
        return switch (this) {
            case RESERVE_INVENTORY -> RELEASE_INVENTORY;
            case AUTHORIZE_PAYMENT -> VOID_PAYMENT;
            // A fraud decision changes nothing outside Order.
            case FRAUD_CHECK -> null;
            // The authorization's VoidPayment still runs after the refund; Payment treats it as a no-op
            // once the capture is refunded, and it releases the hold if the capture never happened.
            case CAPTURE_PAYMENT -> REFUND_PAYMENT;
            case CREATE_SHIPMENT -> CANCEL_SHIPMENT;
            default -> throw new IllegalStateException(this + " is not a forward step");
        };
    }

    /** State once this forward step has succeeded. */
    public SagaState stateAfter() {
        return switch (this) {
            case RESERVE_INVENTORY -> SagaState.INVENTORY_RESERVED;
            case AUTHORIZE_PAYMENT -> SagaState.PAYMENT_AUTHORIZED;
            case FRAUD_CHECK -> SagaState.FRAUD_APPROVED;
            case CAPTURE_PAYMENT -> SagaState.PAYMENT_CAPTURED;
            case CREATE_SHIPMENT -> SagaState.COMPLETED;
            default -> throw new IllegalStateException(this + " is not a forward step");
        };
    }

    /**
     * State recorded while this forward step awaits its reply, given the previous step's result.
     * Normally that result itself; fulfillment shows FULFILLING (PAYMENT_CAPTURED → FULFILLING in the
     * transaction that sends CreateShipment, ADR 0006).
     */
    public SagaState stateWhileAwaiting(SagaState previous) {
        return this == CREATE_SHIPMENT ? SagaState.FULFILLING : previous;
    }
}
