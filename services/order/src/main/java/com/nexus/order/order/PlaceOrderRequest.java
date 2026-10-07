package com.nexus.order.order;

import java.util.List;

/** Body of {@code POST /orders}. Prices are supplied by the client (simulated catalog). */
public record PlaceOrderRequest(String customerId, List<Line> items, String currency, String paymentMethod) {

    public record Line(String sku, Integer quantity, Long unitPriceCents) {
    }
}
