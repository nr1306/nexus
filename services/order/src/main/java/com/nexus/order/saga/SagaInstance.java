package com.nexus.order.saga;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A row of {@code saga_instances}. Immutable; transitions return a copy that the orchestrator saves.
 *
 * @param step          the step awaiting a reply; {@code null} once terminal
 * @param compensations compensation steps still to run after {@code step}, in order
 */
public record SagaInstance(
        UUID sagaId,
        UUID orderId,
        SagaState state,
        SagaStep step,
        Instant stepDeadline,
        int stepAttempts,
        List<SagaStep> compensations,
        String failureReason,
        Instant createdAt) {

    public SagaInstance {
        compensations = List.copyOf(compensations);
    }

    public SagaInstance awaiting(SagaState newState, SagaStep newStep, List<SagaStep> remaining, Instant deadline) {
        return new SagaInstance(sagaId, orderId, newState, newStep, deadline, 1, remaining, failureReason, createdAt);
    }

    public SagaInstance retried(Instant deadline) {
        return new SagaInstance(sagaId, orderId, state, step, deadline, stepAttempts + 1, compensations, failureReason, createdAt);
    }

    public SagaInstance failing(String reason) {
        return new SagaInstance(sagaId, orderId, state, step, stepDeadline, stepAttempts, compensations, reason, createdAt);
    }

    public SagaInstance terminal(SagaState terminalState) {
        return new SagaInstance(sagaId, orderId, terminalState, null, null, stepAttempts, List.of(), failureReason, createdAt);
    }
}
