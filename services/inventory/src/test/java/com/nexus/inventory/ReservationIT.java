package com.nexus.inventory;

import com.nexus.inventory.messaging.InventoryCommandHandler;
import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the command handler (idempotency + reservation logic) directly against Postgres.
 */
class ReservationIT extends InventoryIntegrationTest {

    @Autowired
    InventoryCommandHandler handler;

    @Autowired
    EnvelopeMapper envelopeMapper;

    @Test
    void reservesAllItemsAndRepliesInventoryReserved() {
        String a = newSku(10);
        String b = newSku(5);
        UUID orderId = UUID.randomUUID();
        EventEnvelope command = reserveCommand(orderId, Map.of(a, 3, b, 5));

        handler.handle(command);

        assertThat(available(a)).isEqualTo(7);
        assertThat(reserved(a)).isEqualTo(3);
        assertThat(available(b)).isZero();
        assertThat(reserved(b)).isEqualTo(5);

        EventEnvelope reply = singleReply(orderId);
        assertThat(reply.eventType()).isEqualTo("InventoryReserved");
        assertThat(reply.sagaId()).isEqualTo(command.sagaId());
        assertThat(reply.payload().get("items")).hasSize(2);
        assertThat(reply.payload().get("expiresAt").asText()).isNotBlank();
        assertThat(outboxTopic(reply)).isEqualTo("inventory.events");
    }

    @Test
    void outOfStockRejectsWholeOrderAndLeavesStockUntouched() {
        String plenty = newSku(10);
        String scarce = newSku(1);
        UUID orderId = UUID.randomUUID();
        var items = new LinkedHashMap<String, Integer>();
        items.put(plenty, 4);
        items.put(scarce, 2);

        handler.handle(reserveCommand(orderId, items));

        assertThat(available(plenty)).isEqualTo(10);
        assertThat(reserved(plenty)).isZero();
        assertThat(available(scarce)).isEqualTo(1);
        assertThat(reservationCount(orderId)).isZero();

        EventEnvelope reply = singleReply(orderId);
        assertThat(reply.eventType()).isEqualTo("InventoryRejected");
        assertThat(reply.payload().get("reason").asText()).isEqualTo("OUT_OF_STOCK");
        assertThat(reply.payload().get("sku").asText()).isEqualTo(scarce);
    }

    @Test
    void unknownSkuIsRejected() {
        UUID orderId = UUID.randomUUID();

        handler.handle(reserveCommand(orderId, Map.of("NO-SUCH-SKU-" + UUID.randomUUID(), 1)));

        EventEnvelope reply = singleReply(orderId);
        assertThat(reply.eventType()).isEqualTo("InventoryRejected");
        assertThat(reply.payload().get("reason").asText()).isEqualTo("UNKNOWN_SKU");
    }

    @Test
    void invalidQuantityIsRejectedAsInvalidRequest() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();

        handler.handle(reserveCommand(orderId, Map.of(sku, 0)));

        assertThat(available(sku)).isEqualTo(10);
        assertThat(singleReply(orderId).payload().get("reason").asText()).isEqualTo("INVALID_REQUEST");
    }

    @Test
    void duplicateDeliveryOfSameCommandReservesOnce() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        EventEnvelope command = reserveCommand(orderId, Map.of(sku, 3));

        assertThat(handler.handle(command)).isTrue();
        assertThat(handler.handle(command)).isFalse();

        assertThat(available(sku)).isEqualTo(7);
        assertThat(reserved(sku)).isEqualTo(3);
        assertThat(replies(orderId)).hasSize(1);
    }

    @Test
    void retriedReserveWithNewEventIdRepliesWithExistingHold() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();

        handler.handle(reserveCommand(orderId, Map.of(sku, 3)));
        handler.handle(reserveCommand(orderId, Map.of(sku, 3)));

        assertThat(available(sku)).isEqualTo(7);
        assertThat(replies(orderId)).extracting(EventEnvelope::eventType)
                .containsExactly("InventoryReserved", "InventoryReserved");
    }

    @Test
    void releaseReturnsStockAndSecondReleaseIsNoOp() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 4)));

        handler.handle(releaseCommand(orderId));
        handler.handle(releaseCommand(orderId));

        assertThat(available(sku)).isEqualTo(10);
        assertThat(reserved(sku)).isZero();
        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType)
                .containsExactly("InventoryReserved", "InventoryReleased", "InventoryReleased");
        assertThat(replies.get(1).payload().get("items")).hasSize(1);
        assertThat(replies.get(2).payload().get("items")).isEmpty();
    }

    @Test
    void duplicateDeliveryOfReleaseReleasesOnce() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 4)));
        EventEnvelope release = releaseCommand(orderId);

        assertThat(handler.handle(release)).isTrue();
        assertThat(handler.handle(release)).isFalse();

        assertThat(available(sku)).isEqualTo(10);
        assertThat(reserved(sku)).isZero();
        assertThat(replies(orderId)).hasSize(2);
    }

    @Test
    void releaseWithoutReservationIsNoOpAndStillReplies() {
        UUID orderId = UUID.randomUUID();

        handler.handle(releaseCommand(orderId));

        EventEnvelope reply = singleReply(orderId);
        assertThat(reply.eventType()).isEqualTo("InventoryReleased");
        assertThat(reply.payload().get("items")).isEmpty();
    }

    @Test
    void reserveAfterReleaseIsRejectedAsAlreadyReleased() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 2)));
        handler.handle(releaseCommand(orderId));

        handler.handle(reserveCommand(orderId, Map.of(sku, 2)));

        assertThat(available(sku)).isEqualTo(10);
        EventEnvelope last = replies(orderId).getLast();
        assertThat(last.eventType()).isEqualTo("InventoryRejected");
        assertThat(last.payload().get("reason").asText()).isEqualTo("ALREADY_RELEASED");
    }

    @Test
    void commitSettlesHeldStockAndIsIdempotent() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 4)));
        EventEnvelope commit = commitCommand(orderId);

        assertThat(handler.handle(commit)).isTrue();
        assertThat(handler.handle(commit)).isFalse();
        handler.handle(commitCommand(orderId));

        assertThat(available(sku)).isEqualTo(6);
        assertThat(reserved(sku)).isZero();
        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType)
                .containsExactly("InventoryReserved", "InventoryCommitted", "InventoryCommitted");
        assertThat(replies.get(1).payload().get("items")).hasSize(1);
        assertThat(replies.get(2).payload().get("items")).isEmpty();
    }

    @Test
    void committedStockIsNeverReleased() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 4)));
        handler.handle(commitCommand(orderId));

        handler.handle(releaseCommand(orderId));
        handler.handle(reserveCommand(orderId, Map.of(sku, 4)));

        assertThat(available(sku)).isEqualTo(6);
        assertThat(reserved(sku)).isZero();
        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies.get(2).eventType()).isEqualTo("InventoryReleased");
        assertThat(replies.get(2).payload().get("items")).isEmpty();
        assertThat(replies.get(3).eventType()).isEqualTo("InventoryReserved");
    }

    @Test
    void concurrentReservationsNeverOversell() throws Exception {
        int stock = 10;
        int orders = 50;
        String sku = newSku(stock);
        List<UUID> orderIds = new ArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < orders; i++) {
            UUID orderId = UUID.randomUUID();
            orderIds.add(orderId);
            EventEnvelope command = reserveCommand(orderId, Map.of(sku, 1));
            tasks.add(() -> {
                start.await();
                return handler.handle(command);
            });
        }

        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            List<Future<Boolean>> futures = tasks.stream().map(pool::submit).toList();
            start.countDown();
            for (Future<Boolean> f : futures) {
                assertThat(f.get()).isTrue();
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(available(sku)).isZero();
        assertThat(reserved(sku)).isEqualTo(stock);
        long reservedReplies = orderIds.stream()
                .map(this::singleReply)
                .filter(r -> r.eventType().equals("InventoryReserved"))
                .count();
        assertThat(reservedReplies).isEqualTo(stock);
        assertThat(jdbc.queryForObject("SELECT coalesce(sum(quantity), 0) FROM reservations WHERE sku = ? AND status = 'HELD'",
                Integer.class, sku)).isEqualTo(stock);
    }

    private List<EventEnvelope> replies(UUID orderId) {
        return jdbc.query("SELECT payload::text FROM outbox WHERE aggregate_id = ? ORDER BY created_at, payload->>'occurredAt'",
                (rs, i) -> envelopeMapper.fromJson(rs.getString(1)), orderId.toString());
    }

    private EventEnvelope singleReply(UUID orderId) {
        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).hasSize(1);
        return replies.getFirst();
    }

    private String outboxTopic(EventEnvelope event) {
        return jdbc.queryForObject("SELECT topic FROM outbox WHERE id = ?", String.class, event.eventId());
    }

    private int reservationCount(UUID orderId) {
        return jdbc.queryForObject("SELECT count(*) FROM reservations WHERE order_id = ?", Integer.class, orderId);
    }
}
