package com.nexus.order.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.outbox.OutboxWriter;
import com.nexus.order.config.OrderProperties;
import com.nexus.order.messaging.OrderTopics;
import com.nexus.order.order.OrderDetails;
import com.nexus.order.order.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Orchestrates each order's saga (ADR 0003). Sends one command at a time and waits for its reply;
 * on failure or timeout, runs compensations for completed steps in reverse order, one at a time.
 *
 * <p>Every method runs inside the caller's transaction with the saga row locked, so the new saga
 * state and the command it emits (via the outbox) commit together (CLAUDE.md rules 1 and 9).
 */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);
    private static final String AGGREGATE_TYPE = "order";
    private static final int SCHEMA_VERSION = 1;

    private final SagaRepository sagas;
    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final ObjectMapper objectMapper;
    private final SagaMetrics metrics;
    private final OrderProperties properties;
    private final Clock clock;

    public SagaOrchestrator(SagaRepository sagas, OrderRepository orders, OutboxWriter outbox, ObjectMapper objectMapper,
                            SagaMetrics metrics, OrderProperties properties, Clock clock) {
        this.sagas = sagas;
        this.orders = orders;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.properties = properties;
        this.clock = clock;
    }

    /** Starts the saga for a new order: persists it, announces the order and sends the first command. */
    public void start(OrderDetails order, UUID sagaId) {
        Instant now = clock.instant();
        SagaStep first = SagaStep.FORWARD.getFirst();
        SagaInstance saga = new SagaInstance(sagaId, order.orderId(), SagaState.PENDING, first,
                now.plus(properties.stepTimeout()), 1, List.of(), null, now);
        sagas.insert(saga);

        ObjectNode created = objectMapper.createObjectNode().put("customerId", order.customerId());
        created.set("items", objectMapper.valueToTree(order.items()));
        created.put("totalCents", order.totalCents()).put("currency", order.currency());
        publishOrderEvent(saga, "OrderCreated", created);
        send(saga, order);
    }

    /** Applies a reply from Inventory or Payment. Stale, duplicate or unexpected replies are ignored. */
    public void onReply(EventEnvelope reply) {
        Optional<SagaInstance> found = sagas.lockByOrderId(reply.orderId());
        if (found.isEmpty()) {
            log.warn("Ignoring {}: no saga for this order", reply.eventType());
            return;
        }
        SagaInstance saga = found.get();
        if (!saga.sagaId().equals(reply.sagaId())) {
            log.warn("Ignoring {}: belongs to saga {}", reply.eventType(), reply.sagaId());
            return;
        }
        SagaStep step = saga.step();
        if (step == null) {
            log.info("Ignoring {}: saga already {}", reply.eventType(), saga.state());
            return;
        }
        if (step.isSuccess(reply.eventType())) {
            onStepSucceeded(saga);
        } else if (step.isFailure(reply.eventType())) {
            onStepFailed(saga, reply);
        } else {
            log.info("Ignoring {} while awaiting {}", reply.eventType(), step);
        }
    }

    /**
     * The awaited reply is overdue. Re-send the command (receivers are idempotent per order) until
     * the attempt limit, then compensate (forward step) or give up (compensation step).
     */
    public void onTimeout(SagaInstance saga) {
        SagaStep step = saga.step();
        int maxAttempts = step.isCompensation() ? properties.maxCompensationAttempts() : properties.maxStepAttempts();
        if (saga.stepAttempts() < maxAttempts) {
            SagaInstance retried = saga.retried(deadline());
            sagas.update(retried);
            send(retried, orderOf(saga));
            log.warn("{} timed out; re-sent (attempt {}/{})", step, retried.stepAttempts(), maxAttempts);
        } else if (!step.isCompensation()) {
            startCompensation(saga, "TIMEOUT_" + step.name(), true);
        } else {
            needsAttention(saga, "COMPENSATION_TIMEOUT_" + step.name());
        }
    }

    private void onStepSucceeded(SagaInstance saga) {
        SagaStep step = saga.step();
        if (step.isCompensation()) {
            continueCompensation(saga);
            return;
        }
        SagaStep next = step.next();
        if (next != null) {
            SagaInstance advanced = saga.awaiting(step.stateAfter(), next, List.of(), deadline());
            sagas.update(advanced);
            send(advanced, orderOf(saga));
            log.info("{} succeeded; now {}", step, advanced.state());
            return;
        }
        complete(saga);
    }

    private void onStepFailed(SagaInstance saga, EventEnvelope reply) {
        String reason = reply.payload().path("reason").asText(reply.eventType());
        if (saga.step().isCompensation()) {
            needsAttention(saga, saga.step().name() + "_FAILED_" + reason);
        } else {
            startCompensation(saga, reason, false);
        }
    }

    private void complete(SagaInstance saga) {
        SagaInstance completed = saga.terminal(SagaState.COMPLETED);
        sagas.update(completed);
        OrderDetails order = orderOf(saga);
        // Settle the stock hold; no reply is awaited (InventoryCommitted is informational).
        sendCommand(saga, OrderTopics.INVENTORY_COMMANDS, "CommitInventory", objectMapper.createObjectNode());
        publishOrderEvent(saga, "OrderCompleted", objectMapper.createObjectNode()
                .put("totalCents", order.totalCents()).put("currency", order.currency()));
        metrics.completed(Duration.between(saga.createdAt(), clock.instant()));
        log.info("Saga COMPLETED");
    }

    /**
     * Undoes completed forward steps in reverse order. On a timeout the in-flight step may also have
     * succeeded, so it is undone too (compensations are idempotent, so undoing something that never
     * happened is a no-op).
     */
    private void startCompensation(SagaInstance saga, String reason, boolean includeInFlightStep) {
        List<SagaStep> plan = compensationPlan(saga.step(), includeInFlightStep);
        SagaInstance failing = saga.failing(reason);
        if (plan.isEmpty()) {
            cancel(failing, false);
            return;
        }
        SagaInstance compensating = failing.awaiting(SagaState.COMPENSATING, plan.getFirst(),
                plan.subList(1, plan.size()), deadline());
        sagas.update(compensating);
        send(compensating, orderOf(saga));
        metrics.compensating(reason);
        log.info("{} failed ({}); compensating with {}", saga.step(), reason, plan);
    }

    private void continueCompensation(SagaInstance saga) {
        if (saga.compensations().isEmpty()) {
            cancel(saga, true);
            return;
        }
        List<SagaStep> remaining = saga.compensations();
        SagaInstance next = saga.awaiting(SagaState.COMPENSATING, remaining.getFirst(),
                remaining.subList(1, remaining.size()), deadline());
        sagas.update(next);
        send(next, orderOf(saga));
    }

    private void cancel(SagaInstance saga, boolean compensated) {
        SagaInstance cancelled = saga.terminal(SagaState.CANCELLED);
        sagas.update(cancelled);
        publishOrderEvent(saga, "OrderCancelled", objectMapper.createObjectNode().put("reason", saga.failureReason()));
        if (compensated) {
            metrics.compensated(saga.failureReason());
        }
        metrics.cancelled(saga.failureReason());
        log.info("Saga CANCELLED ({})", saga.failureReason());
    }

    private void needsAttention(SagaInstance saga, String reason) {
        SagaInstance stuck = saga.failing(reason).terminal(SagaState.NEEDS_ATTENTION);
        sagas.update(stuck);
        publishOrderEvent(saga, "OrderNeedsAttention", objectMapper.createObjectNode().put("reason", reason));
        metrics.needsAttention(reason);
        log.error("Saga NEEDS_ATTENTION ({})", reason);
    }

    static List<SagaStep> compensationPlan(SagaStep failedStep, boolean includeFailedStep) {
        List<SagaStep> toUndo = new ArrayList<>(SagaStep.FORWARD.subList(0, SagaStep.FORWARD.indexOf(failedStep)));
        if (includeFailedStep) {
            toUndo.add(failedStep);
        }
        Collections.reverse(toUndo);
        LinkedHashSet<SagaStep> plan = new LinkedHashSet<>();
        for (SagaStep step : toUndo) {
            if (step.undo() != null) {
                plan.add(step.undo());
            }
        }
        return List.copyOf(plan);
    }

    private void send(SagaInstance saga, OrderDetails order) {
        SagaStep step = saga.step();
        ObjectNode payload = objectMapper.createObjectNode();
        switch (step) {
            case RESERVE_INVENTORY -> payload.set("items", objectMapper.valueToTree(order.items().stream()
                    .map(i -> objectMapper.createObjectNode().put("sku", i.sku()).put("quantity", i.quantity()))
                    .toList()));
            case AUTHORIZE_PAYMENT -> payload.put("amountCents", order.totalCents())
                    .put("currency", order.currency())
                    .put("paymentMethod", order.paymentMethod());
            case CAPTURE_PAYMENT -> { }
            case VOID_PAYMENT, RELEASE_INVENTORY -> payload.put("reason", saga.failureReason());
        }
        sendCommand(saga, step.topic(), step.commandType(), payload);
    }

    private void sendCommand(SagaInstance saga, String topic, String commandType, ObjectNode payload) {
        EventEnvelope command = EventEnvelope.create(commandType, SCHEMA_VERSION, saga.orderId(), saga.sagaId(), payload, clock);
        outbox.append(topic, AGGREGATE_TYPE, command);
    }

    private void publishOrderEvent(SagaInstance saga, String eventType, ObjectNode payload) {
        EventEnvelope event = EventEnvelope.create(eventType, SCHEMA_VERSION, saga.orderId(), saga.sagaId(), payload, clock);
        outbox.append(OrderTopics.ORDER_EVENTS, AGGREGATE_TYPE, event);
    }

    private OrderDetails orderOf(SagaInstance saga) {
        return orders.findDetails(saga.orderId())
                .orElseThrow(() -> new IllegalStateException("Saga " + saga.sagaId() + " has no order"));
    }

    private Instant deadline() {
        return clock.instant().plus(properties.stepTimeout());
    }
}
