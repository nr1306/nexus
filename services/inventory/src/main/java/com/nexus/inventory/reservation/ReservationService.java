package com.nexus.inventory.reservation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.inventory.config.InventoryProperties;
import com.nexus.inventory.messaging.InventoryTopics;
import com.nexus.inventory.reservation.InventoryMessages.InventoryCommitted;
import com.nexus.inventory.reservation.InventoryMessages.InventoryRejected;
import com.nexus.inventory.reservation.InventoryMessages.InventoryReleased;
import com.nexus.inventory.reservation.InventoryMessages.InventoryReserved;
import com.nexus.inventory.reservation.InventoryMessages.RejectionReason;
import com.nexus.inventory.reservation.InventoryMessages.ReserveInventory;
import com.nexus.inventory.reservation.ReservationRepository.Reservation;
import com.nexus.inventory.reservation.ReservationRepository.Status;
import com.nexus.inventory.stock.StockRepository;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reserves and releases stock for orders. Every method runs inside the caller's transaction (the
 * idempotent-consumer transaction), so the stock change, the reservation rows and the reply in the
 * outbox commit or roll back together.
 */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);
    private static final String AGGREGATE_TYPE = "reservation";
    private static final int SCHEMA_VERSION = 1;

    private final StockRepository stock;
    private final ReservationRepository reservations;
    private final OutboxWriter outbox;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final InventoryProperties properties;

    public ReservationService(StockRepository stock, ReservationRepository reservations, OutboxWriter outbox,
                              ObjectMapper objectMapper, Clock clock, InventoryProperties properties) {
        this.stock = stock;
        this.reservations = reservations;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.properties = properties;
    }

    /**
     * Reserves every item or none. A repeated command for an order that already holds stock replies
     * with the existing hold instead of reserving again.
     */
    public void reserve(EventEnvelope command) {
        List<LineItem> requested = parseReserve(command);
        if (requested == null) {
            reject(command, RejectionReason.INVALID_REQUEST, null);
            return;
        }

        List<Reservation> existing = reservations.findByOrder(command.orderId());
        if (!existing.isEmpty()) {
            replyToRepeatedReserve(command, existing);
            return;
        }

        List<LineItem> items = LineItem.mergeAndSort(requested);
        List<LineItem> taken = new ArrayList<>();
        for (LineItem item : items) {
            if (!stock.tryReserve(item.sku(), item.quantity())) {
                undo(taken);
                RejectionReason reason = stock.exists(item.sku()) ? RejectionReason.OUT_OF_STOCK : RejectionReason.UNKNOWN_SKU;
                reject(command, reason, item.sku());
                return;
            }
            taken.add(item);
        }

        Instant expiresAt = clock.instant().plus(properties.reservationTtl());
        reservations.insertHeld(command.orderId(), items, expiresAt);
        reply(command, InventoryMessages.INVENTORY_RESERVED, new InventoryReserved(items, expiresAt));
        log.info("Reserved {} SKU(s) until {}", items.size(), expiresAt);
    }

    /**
     * Returns all held stock for the order. Releasing twice, or releasing an order with no hold, is a
     * no-op that still replies, so the saga can always complete its compensation.
     */
    public void release(EventEnvelope command) {
        List<LineItem> released = LineItem.sortBySku(reservations.releaseHeld(command.orderId(), clock.instant()));
        for (LineItem item : released) {
            stock.unreserve(item.sku(), item.quantity());
        }
        reply(command, InventoryMessages.INVENTORY_RELEASED, new InventoryReleased(released));
        log.info("Released {} SKU(s)", released.size());
    }

    /**
     * The order completed: held stock is sold. Committing twice, or with no hold, is a no-op that
     * still replies. Committed holds are never released or expired.
     */
    public void commit(EventEnvelope command) {
        List<LineItem> committed = LineItem.sortBySku(reservations.commitHeld(command.orderId(), clock.instant()));
        for (LineItem item : committed) {
            stock.commit(item.sku(), item.quantity());
        }
        reply(command, InventoryMessages.INVENTORY_COMMITTED, new InventoryCommitted(committed));
        log.info("Committed {} SKU(s)", committed.size());
    }

    private void replyToRepeatedReserve(EventEnvelope command, List<Reservation> existing) {
        List<Reservation> held = existing.stream().filter(r -> r.status() != Status.RELEASED).toList();
        if (held.isEmpty()) {
            reject(command, RejectionReason.ALREADY_RELEASED, null);
            return;
        }
        List<LineItem> items = held.stream().map(r -> new LineItem(r.sku(), r.quantity())).toList();
        reply(command, InventoryMessages.INVENTORY_RESERVED, new InventoryReserved(items, held.getFirst().expiresAt()));
        log.info("Order already holds or committed stock; repeated InventoryReserved");
    }

    private void undo(List<LineItem> taken) {
        for (LineItem item : taken) {
            stock.unreserve(item.sku(), item.quantity());
        }
    }

    private void reject(EventEnvelope command, RejectionReason reason, String sku) {
        reply(command, InventoryMessages.INVENTORY_REJECTED, new InventoryRejected(reason, sku));
        log.info("Rejected reservation: {} (sku={})", reason, sku);
    }

    private void reply(EventEnvelope command, String eventType, Object payload) {
        EventEnvelope event = EventEnvelope.create(eventType, SCHEMA_VERSION, command.orderId(), command.sagaId(),
                objectMapper.valueToTree(payload), clock);
        outbox.append(InventoryTopics.EVENTS, AGGREGATE_TYPE, event);
    }

    /** @return the requested items, or {@code null} if the payload is not a valid ReserveInventory */
    private List<LineItem> parseReserve(EventEnvelope command) {
        ReserveInventory request;
        try {
            request = objectMapper.treeToValue(command.payload(), ReserveInventory.class);
        } catch (JsonProcessingException e) {
            return null;
        }
        if (request.items() == null || request.items().isEmpty()) {
            return null;
        }
        for (LineItem item : request.items()) {
            if (item == null || item.sku() == null || item.sku().isBlank() || item.quantity() <= 0) {
                return null;
            }
        }
        return request.items();
    }
}
