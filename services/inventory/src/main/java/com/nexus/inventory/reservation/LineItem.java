package com.nexus.inventory.reservation;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public record LineItem(String sku, int quantity) {

    /**
     * Sums quantities per SKU and sorts by SKU, so stock rows are always locked in the same order
     * (no deadlocks between concurrent multi-SKU reservations).
     */
    static List<LineItem> mergeAndSort(Collection<LineItem> items) {
        Map<String, Integer> bySku = new TreeMap<>();
        for (LineItem item : items) {
            bySku.merge(item.sku(), item.quantity(), Integer::sum);
        }
        return bySku.entrySet().stream()
                .map(e -> new LineItem(e.getKey(), e.getValue()))
                .toList();
    }

    static List<LineItem> sortBySku(Collection<LineItem> items) {
        return items.stream().sorted(Comparator.comparing(LineItem::sku)).toList();
    }
}
