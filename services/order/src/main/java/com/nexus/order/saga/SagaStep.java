package com.nexus.order.saga;

import com.nexus.order.messaging.OrderTopics;

import java.util.List;

/**
 * A command the saga sends and waits on, with the replies that end the wait.
 * Forward steps run in declaration order; compensation steps undo them (ADR 0003).
 */
public enum SagaStep {

    RESERVE_INVENTORY("ReserveInventory", OrderTopics.INVENTORY_COMMANDS, "InventoryReserved", "InventoryRejected", false),
    AUTHORIZE_PAYMENT("AuthorizePayment", OrderTopics.PAYMENT_COMMANDS, "PaymentAuthorized", "PaymentDeclined", false),
    CAPTURE_PAYMENT("CapturePayment", OrderTopics.PAYMENT_COMMANDS, "PaymentCaptured", "CaptureFailed", false),

    VOID_PAYMENT("VoidPayment", OrderTopics.PAYMENT_COMMANDS, "PaymentVoided", "PaymentVoidFailed", true),
    RELEASE_INVENTORY("ReleaseInventory", OrderTopics.INVENTORY_COMMANDS, "InventoryReleased", null, true);

    /** Forward steps in execution order. */
    public static final List<SagaStep> FORWARD = List.of(RESERVE_INVENTORY, AUTHORIZE_PAYMENT, CAPTURE_PAYMENT);

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
        return successReply.equals(replyType);
    }

    public boolean isFailure(String replyType) {
        return failureReply != null && failureReply.equals(replyType);
    }

    public boolean isCompensation() {
        return compensation;
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
            // Capture is undone by a refund (Phase 2). Until then the authorization void below it
            // runs and fails with ALREADY_CAPTURED → NEEDS_ATTENTION.
            case CAPTURE_PAYMENT -> null;
            default -> throw new IllegalStateException(this + " is not a forward step");
        };
    }

    /** State once this forward step has succeeded. */
    public SagaState stateAfter() {
        return switch (this) {
            case RESERVE_INVENTORY -> SagaState.INVENTORY_RESERVED;
            case AUTHORIZE_PAYMENT -> SagaState.PAYMENT_AUTHORIZED;
            case CAPTURE_PAYMENT -> SagaState.COMPLETED;
            default -> throw new IllegalStateException(this + " is not a forward step");
        };
    }
}
