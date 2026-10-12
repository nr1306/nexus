package com.nexus.reconciliation.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.nexus.messaging.envelope.EventEnvelope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/**
 * Turns events into facts on the order's projection row (ADR 0009). Replies that say nothing
 * happened ({@code refunded=false}, {@code voided=false}, {@code cancelled=false}) and rejections
 * record no fact, only that the order had activity.
 */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class ProjectionService {

    private final ProjectionRepository projections;
    private final Clock clock;

    ProjectionService(ProjectionRepository projections, Clock clock) {
        this.projections = projections;
        this.clock = clock;
    }

    public void apply(EventEnvelope event) {
        projections.record(event.orderId(), event.sagaId(), factOf(event), event.occurredAt(), clock.instant());
    }

    static Fact factOf(EventEnvelope event) {
        JsonNode payload = event.payload();
        return switch (event.eventType()) {
            case "OrderCreated" -> Fact.CREATED;
            case "OrderCompleted" -> Fact.COMPLETED;
            case "OrderCancelled" -> Fact.CANCELLED;
            case "OrderNeedsAttention" -> Fact.NEEDS_ATTENTION;
            case "InventoryReserved" -> Fact.RESERVED;
            case "InventoryReleased" -> Fact.RELEASED;
            case "InventoryCommitted" -> Fact.COMMITTED;
            case "ReservationExpired" -> Fact.EXPIRED;
            case "PaymentAuthorized" -> Fact.AUTHORIZED;
            case "PaymentCaptured" -> Fact.CAPTURED;
            case "PaymentRefunded" -> flag(payload, "refunded") ? Fact.REFUNDED : null;
            case "PaymentVoided" -> flag(payload, "voided") ? Fact.VOIDED : null;
            case "ShipmentCreated" -> Fact.SHIPMENT_CREATED;
            case "ShipmentCancelled" -> flag(payload, "cancelled") ? Fact.SHIPMENT_CANCELLED : null;
            case "ShipmentShipped" -> Fact.SHIPPED;
            default -> null;
        };
    }

    private static boolean flag(JsonNode payload, String field) {
        return payload != null && payload.path(field).asBoolean(false);
    }
}
