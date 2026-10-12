package com.nexus.order.saga;

import com.nexus.order.messaging.ReplyConsumptionGate;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaTimeoutSchedulingTest {

    private final SagaTimeoutSweeper sweeper = mock(SagaTimeoutSweeper.class);
    private final ReplyConsumptionGate gate = mock(ReplyConsumptionGate.class);
    private final SagaTimeoutScheduling scheduling = new SagaTimeoutScheduling(sweeper, gate);

    @Test
    void doesNotSweepBeforeRepliesAreBeingRead() {
        when(gate.timeoutsAllowed()).thenReturn(false);

        scheduling.sweep();

        verify(sweeper, never()).sweep();
    }

    @Test
    void sweepsOnceTheGateIsOpen() {
        when(gate.timeoutsAllowed()).thenReturn(true);

        scheduling.sweep();

        verify(sweeper).sweep();
    }
}
