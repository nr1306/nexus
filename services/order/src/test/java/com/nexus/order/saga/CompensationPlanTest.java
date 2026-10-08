package com.nexus.order.saga;

import org.junit.jupiter.api.Test;

import static com.nexus.order.saga.SagaStep.AUTHORIZE_PAYMENT;
import static com.nexus.order.saga.SagaStep.CANCEL_SHIPMENT;
import static com.nexus.order.saga.SagaStep.CAPTURE_PAYMENT;
import static com.nexus.order.saga.SagaStep.CREATE_SHIPMENT;
import static com.nexus.order.saga.SagaStep.FRAUD_CHECK;
import static com.nexus.order.saga.SagaStep.REFUND_PAYMENT;
import static com.nexus.order.saga.SagaStep.RELEASE_INVENTORY;
import static com.nexus.order.saga.SagaStep.RESERVE_INVENTORY;
import static com.nexus.order.saga.SagaStep.VOID_PAYMENT;
import static org.assertj.core.api.Assertions.assertThat;

/** The compensation table in CLAUDE.md, plus timeouts (which also undo the in-flight step). */
class CompensationPlanTest {

    @Test
    void reserveRejectedNeedsNoCompensation() {
        assertThat(SagaOrchestrator.compensationPlan(RESERVE_INVENTORY, false)).isEmpty();
    }

    @Test
    void reserveTimeoutReleasesPossibleHold() {
        assertThat(SagaOrchestrator.compensationPlan(RESERVE_INVENTORY, true)).containsExactly(RELEASE_INVENTORY);
    }

    @Test
    void paymentDeclinedReleasesInventory() {
        assertThat(SagaOrchestrator.compensationPlan(AUTHORIZE_PAYMENT, false)).containsExactly(RELEASE_INVENTORY);
    }

    @Test
    void authorizeTimeoutVoidsThenReleases() {
        assertThat(SagaOrchestrator.compensationPlan(AUTHORIZE_PAYMENT, true)).containsExactly(VOID_PAYMENT, RELEASE_INVENTORY);
    }

    @Test
    void fraudRejectedOrTimedOutVoidsThenReleases() {
        assertThat(SagaOrchestrator.compensationPlan(FRAUD_CHECK, false)).containsExactly(VOID_PAYMENT, RELEASE_INVENTORY);
        assertThat(SagaOrchestrator.compensationPlan(FRAUD_CHECK, true)).containsExactly(VOID_PAYMENT, RELEASE_INVENTORY);
    }

    @Test
    void captureFailedVoidsThenReleases() {
        assertThat(SagaOrchestrator.compensationPlan(CAPTURE_PAYMENT, false)).containsExactly(VOID_PAYMENT, RELEASE_INVENTORY);
    }

    @Test
    void captureTimeoutRefundsThenVoidsThenReleases() {
        // The capture may or may not have happened: the refund covers one case, the void the other.
        assertThat(SagaOrchestrator.compensationPlan(CAPTURE_PAYMENT, true))
                .containsExactly(REFUND_PAYMENT, VOID_PAYMENT, RELEASE_INVENTORY);
    }

    @Test
    void fulfillmentFailedRefundsThenReleases() {
        // VoidPayment after a refund is a no-op in Payment (ADR 0006).
        assertThat(SagaOrchestrator.compensationPlan(CREATE_SHIPMENT, false))
                .containsExactly(REFUND_PAYMENT, VOID_PAYMENT, RELEASE_INVENTORY);
    }

    @Test
    void fulfillmentTimeoutAlsoCancelsPossibleShipment() {
        assertThat(SagaOrchestrator.compensationPlan(CREATE_SHIPMENT, true))
                .containsExactly(CANCEL_SHIPMENT, REFUND_PAYMENT, VOID_PAYMENT, RELEASE_INVENTORY);
    }
}
