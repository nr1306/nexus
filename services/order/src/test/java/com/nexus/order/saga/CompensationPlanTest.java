package com.nexus.order.saga;

import org.junit.jupiter.api.Test;

import static com.nexus.order.saga.SagaStep.AUTHORIZE_PAYMENT;
import static com.nexus.order.saga.SagaStep.CAPTURE_PAYMENT;
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
    void captureFailedVoidsThenReleases() {
        assertThat(SagaOrchestrator.compensationPlan(CAPTURE_PAYMENT, false)).containsExactly(VOID_PAYMENT, RELEASE_INVENTORY);
        assertThat(SagaOrchestrator.compensationPlan(CAPTURE_PAYMENT, true)).containsExactly(VOID_PAYMENT, RELEASE_INVENTORY);
    }
}
