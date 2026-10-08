package com.nexus.fulfillment.shipment;


import java.util.List;
import java.util.UUID;

/**
 * Payloads of fulfillment commands and events, and the order events Fulfillment reacts to.
 * Schemas: {@code contracts/events/fulfillment/}, {@code contracts/events/order/}.
 */
public final class FulfillmentMessages {

    public static final String CREATE_SHIPMENT = "CreateShipment";
    public static final String CANCEL_SHIPMENT = "CancelShipment";

    public static final String SHIPMENT_CREATED = "ShipmentCreated";
    public static final String FULFILLMENT_FAILED = "FulfillmentFailed";
    public static final String SHIPMENT_CANCELLED = "ShipmentCancelled";
    public static final String SHIPMENT_CANCEL_FAILED = "ShipmentCancelFailed";
    public static final String SHIPMENT_SHIPPED = "ShipmentShipped";

    public static final String ORDER_COMPLETED = "OrderCompleted";
    public static final String ORDER_CANCELLED = "OrderCancelled";

    /** Failure reasons stored in {@code shipments.failure_reason} and sent in replies. */
    public static final String RESTRICTED_SKU = "RESTRICTED_SKU";
    public static final String INVALID_REQUEST = "INVALID_REQUEST";
    public static final String ORDER_CANCELLED_REASON = "ORDER_CANCELLED";
    public static final String ALREADY_SHIPPED = "ALREADY_SHIPPED";

    private FulfillmentMessages() {
    }

    public record CreateShipment(String customerId, List<Item> items) {
    }

    public record Item(String sku, Integer quantity) {
    }

    public record ShipmentCreated(UUID shipmentId) {
    }

    public record FulfillmentFailed(String reason) {
    }

    public record ShipmentCancelled(boolean cancelled) {
    }

    public record ShipmentCancelFailed(String reason) {
    }

    public record ShipmentShipped(UUID shipmentId, String trackingNumber) {
    }
}
