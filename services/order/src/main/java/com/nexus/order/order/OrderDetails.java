package com.nexus.order.order;

import java.util.List;
import java.util.UUID;

/** What the saga needs to build its commands. */
public record OrderDetails(
        UUID orderId,
        String customerId,
        List<OrderView.Item> items,
        long totalCents,
        String currency,
        String paymentMethod) {
}
