package com.nexus.fulfillment.shipment;

import java.util.List;
import java.util.UUID;

/**
 * A row of {@code shipments}: the order's shipment, or a marker for a rejected request or a cancel
 * that arrived first ({@code shipmentId == null}).
 */
public record Shipment(
        UUID orderId,
        UUID shipmentId,
        Status status,
        String customerId,
        List<FulfillmentMessages.Item> items,
        String failureReason,
        String trackingNumber) {

    public enum Status { CREATED, SHIPPED, CANCELLED, REJECTED }

    public boolean exists() {
        return shipmentId != null;
    }
}
