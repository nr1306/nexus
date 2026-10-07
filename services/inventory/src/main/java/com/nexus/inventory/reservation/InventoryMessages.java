package com.nexus.inventory.reservation;

import java.time.Instant;
import java.util.List;

/**
 * Payloads of inventory commands and events. Schemas: {@code contracts/events/inventory/}.
 */
public final class InventoryMessages {

    public static final String RESERVE_INVENTORY = "ReserveInventory";
    public static final String RELEASE_INVENTORY = "ReleaseInventory";
    public static final String INVENTORY_RESERVED = "InventoryReserved";
    public static final String INVENTORY_REJECTED = "InventoryRejected";
    public static final String INVENTORY_RELEASED = "InventoryReleased";

    private InventoryMessages() {
    }

    public record ReserveInventory(List<LineItem> items) {
    }

    public record ReleaseInventory(String reason) {
    }

    public record InventoryReserved(List<LineItem> items, Instant expiresAt) {
    }

    public record InventoryRejected(RejectionReason reason, String sku) {
    }

    public record InventoryReleased(List<LineItem> items) {
    }

    public enum RejectionReason {
        OUT_OF_STOCK,
        UNKNOWN_SKU,
        INVALID_REQUEST,
        ALREADY_RELEASED
    }
}
