package com.nexus.order.order;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** An order with its saga state, as returned by the API. */
public record OrderView(
        UUID orderId,
        UUID sagaId,
        String status,
        String step,
        String failureReason,
        String customerId,
        List<Item> items,
        long totalCents,
        String currency,
        Instant createdAt,
        Instant updatedAt) {

    public record Item(String sku, int quantity, long unitPriceCents) {
    }
}
