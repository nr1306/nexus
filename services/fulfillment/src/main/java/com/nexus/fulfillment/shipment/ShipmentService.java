package com.nexus.fulfillment.shipment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.fulfillment.messaging.FulfillmentTopics;
import com.nexus.fulfillment.notification.NotificationService;
import com.nexus.fulfillment.notification.NotificationType;
import com.nexus.fulfillment.shipment.FulfillmentMessages.CreateShipment;
import com.nexus.fulfillment.shipment.FulfillmentMessages.FulfillmentFailed;
import com.nexus.fulfillment.shipment.FulfillmentMessages.Item;
import com.nexus.fulfillment.shipment.FulfillmentMessages.ShipmentCancelFailed;
import com.nexus.fulfillment.shipment.FulfillmentMessages.ShipmentCancelled;
import com.nexus.fulfillment.shipment.FulfillmentMessages.ShipmentCreated;
import com.nexus.fulfillment.shipment.FulfillmentMessages.ShipmentShipped;
import com.nexus.fulfillment.shipment.Shipment.Status;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/**
 * Saga step 5 (SPEC.md §4): one shipment per order, created on command, cancelled as compensation and
 * dispatched once the order is COMPLETED. Every method runs inside the caller's idempotent-consumer
 * transaction with the order's {@code shipments} row locked, so the row change and the reply in the
 * outbox commit together.
 *
 * <p>Every outcome is stored as a row (a rejection or an early cancel too) and replies are built from
 * the row, so a retried command (new eventId, same order) gets the same answer, and a CreateShipment
 * arriving after CancelShipment is rejected (ADR 0006).
 */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class ShipmentService {

    private static final Logger log = LoggerFactory.getLogger(ShipmentService.class);
    private static final String AGGREGATE_TYPE = "shipment";
    private static final int SCHEMA_VERSION = 1;

    private final ShipmentRepository shipments;
    private final NotificationService notifications;
    private final OutboxWriter outbox;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public ShipmentService(ShipmentRepository shipments, NotificationService notifications, OutboxWriter outbox,
                           ObjectMapper objectMapper, Clock clock) {
        this.shipments = shipments;
        this.notifications = notifications;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public void create(EventEnvelope command) {
        UUID orderId = command.orderId();
        Shipment shipment = shipments.lock(orderId).orElseGet(() -> {
            CreateShipment request = parseCreate(command);
            if (request == null) {
                return save(rejected(orderId, null, null, FulfillmentMessages.INVALID_REQUEST));
            }
            List<String> restricted = shipments.restricted(request.items().stream().map(Item::sku).toList());
            if (!restricted.isEmpty()) {
                log.info("Carrier refuses SKUs {}", restricted);
                return save(rejected(orderId, request.customerId(), request.items(), FulfillmentMessages.RESTRICTED_SKU));
            }
            return save(new Shipment(orderId, UUID.randomUUID(), Status.CREATED, request.customerId(),
                    request.items(), null, null));
        });
        switch (shipment.status()) {
            case CREATED, SHIPPED -> publish(command, FulfillmentMessages.SHIPMENT_CREATED, new ShipmentCreated(shipment.shipmentId()));
            case REJECTED -> publish(command, FulfillmentMessages.FULFILLMENT_FAILED, new FulfillmentFailed(shipment.failureReason()));
            case CANCELLED -> publish(command, FulfillmentMessages.FULFILLMENT_FAILED,
                    new FulfillmentFailed(FulfillmentMessages.ORDER_CANCELLED_REASON));
        }
    }

    /**
     * Compensation. Cancelling twice, or cancelling an order with no shipment, is a no-op that still
     * replies (cancelled=false) and leaves a CANCELLED marker that rejects a late CreateShipment.
     * A dispatched shipment can't be cancelled.
     */
    public void cancel(EventEnvelope command) {
        UUID orderId = command.orderId();
        Optional<Shipment> found = shipments.lock(orderId);
        if (found.isEmpty()) {
            save(new Shipment(orderId, null, Status.CANCELLED, null, null, null, null));
            publish(command, FulfillmentMessages.SHIPMENT_CANCELLED, new ShipmentCancelled(false));
            return;
        }
        Shipment shipment = found.get();
        if (shipment.status() == Status.SHIPPED) {
            log.warn("Can't cancel: shipment {} already shipped", shipment.shipmentId());
            publish(command, FulfillmentMessages.SHIPMENT_CANCEL_FAILED, new ShipmentCancelFailed(FulfillmentMessages.ALREADY_SHIPPED));
            return;
        }
        if (shipment.status() != Status.CANCELLED) {
            shipments.updateStatus(orderId, Status.CANCELLED, null);
            log.info("Shipment cancelled (was {})", shipment.status());
        }
        publish(command, FulfillmentMessages.SHIPMENT_CANCELLED, new ShipmentCancelled(shipment.exists()));
    }

    /** The saga committed: dispatch the shipment and tell the customer. */
    public void onOrderCompleted(EventEnvelope event) {
        UUID orderId = event.orderId();
        Optional<Shipment> found = shipments.lock(orderId);
        String customerId = customerId(event, found);
        notifications.notify(orderId, NotificationType.ORDER_CONFIRMED, customerId,
                "Your order " + orderId + " is confirmed.");
        if (found.isEmpty() || !found.get().exists()) {
            log.warn("Order completed without a shipment; nothing to dispatch");
            return;
        }
        Shipment shipment = found.get();
        String tracking = shipment.trackingNumber();
        if (shipment.status() == Status.CREATED) {
            tracking = trackingNumber(shipment.shipmentId());
            shipments.updateStatus(orderId, Status.SHIPPED, tracking);
            publish(event, FulfillmentMessages.SHIPMENT_SHIPPED, new ShipmentShipped(shipment.shipmentId(), tracking));
            log.info("Shipment {} shipped ({})", shipment.shipmentId(), tracking);
        } else if (shipment.status() != Status.SHIPPED) {
            log.warn("Order completed but shipment is {}; not dispatching", shipment.status());
            return;
        }
        notifications.notify(orderId, NotificationType.ORDER_SHIPPED, customerId,
                "Your order " + orderId + " has shipped. Tracking number: " + tracking + ".");
    }

    public void onOrderCancelled(EventEnvelope event) {
        UUID orderId = event.orderId();
        String reason = event.payload().path("reason").asText("UNKNOWN");
        notifications.notify(orderId, NotificationType.ORDER_CANCELLED, customerId(event, shipments.lock(orderId)),
                "Your order " + orderId + " was cancelled (" + reason + "). Any payment has been released or refunded.");
    }

    private Shipment rejected(UUID orderId, String customerId, List<Item> items, String reason) {
        return new Shipment(orderId, null, Status.REJECTED, customerId, items, reason, null);
    }

    private Shipment save(Shipment shipment) {
        shipments.insert(shipment);
        log.info("Shipment row {}{}", shipment.status(),
                shipment.failureReason() == null ? "" : " (" + shipment.failureReason() + ")");
        return shipment;
    }

    private void publish(EventEnvelope cause, String eventType, Object payload) {
        EventEnvelope event = EventEnvelope.create(eventType, SCHEMA_VERSION, cause.orderId(), cause.sagaId(),
                objectMapper.valueToTree(payload), clock);
        outbox.append(FulfillmentTopics.EVENTS, AGGREGATE_TYPE, event);
    }

    private static String customerId(EventEnvelope event, Optional<Shipment> shipment) {
        String fromEvent = event.payload().path("customerId").asText(null);
        if (fromEvent != null && !fromEvent.isBlank()) {
            return fromEvent;
        }
        return shipment.map(Shipment::customerId).orElse(null);
    }

    static String trackingNumber(UUID shipmentId) {
        return "TRK" + shipmentId.toString().replace("-", "").substring(0, 12).toUpperCase(Locale.ROOT);
    }

    /** @return the request, or {@code null} if the payload is not a valid CreateShipment */
    private CreateShipment parseCreate(EventEnvelope command) {
        CreateShipment request;
        try {
            request = objectMapper.treeToValue(command.payload(), CreateShipment.class);
        } catch (JsonProcessingException e) {
            return null;
        }
        boolean valid = request.customerId() != null && !request.customerId().isBlank()
                && request.items() != null && !request.items().isEmpty()
                && request.items().stream().allMatch(i -> i != null && i.sku() != null && !i.sku().isBlank()
                && i.quantity() != null && i.quantity() > 0);
        return valid ? request : null;
    }
}
