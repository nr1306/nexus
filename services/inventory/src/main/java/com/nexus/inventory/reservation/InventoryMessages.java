package com.nexus.inventory.reservation;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

/**
 * Payloads of inventory commands and events. Schemas: {@code contracts/events/inventory/}.
 */
public final class InventoryMessages {

    public static final String RESERVE_INVENTORY = "ReserveInventory";
    public static final String RELEASE_INVENTORY = "ReleaseInventory";
    public static final String COMMIT_INVENTORY = "CommitInventory";
    public static final String INVENTORY_RESERVED = "InventoryReserved";
    public static final String INVENTORY_REJECTED = "InventoryRejected";
    public static final String INVENTORY_RELEASED = "InventoryReleased";
    public static final String INVENTORY_COMMITTED = "InventoryCommitted";
    public static final String RESERVATION_EXPIRED = "ReservationExpired";

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

    /** {@code shortfall}: items of an expired hold that could no longer be re-taken at commit (oversold). */
    public record InventoryCommitted(List<LineItem> items,
                                     @JsonInclude(JsonInclude.Include.NON_EMPTY) List<LineItem> shortfall) {
    }

    public record ReservationExpired(List<LineItem> items) {
    }

    public enum RejectionReason {
        OUT_OF_STOCK,
        UNKNOWN_SKU,
        INVALID_REQUEST,
        ALREADY_RELEASED
    }
}
