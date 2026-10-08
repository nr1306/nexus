package com.nexus.fulfillment;

import com.nexus.fulfillment.messaging.FulfillmentHandler;
import com.nexus.messaging.envelope.EventEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the handler (idempotency + shipment rules + notifications) directly against Postgres.
 */
class FulfillmentIT extends FulfillmentIntegrationTest {

    @Autowired
    FulfillmentHandler handler;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    void createShipmentRepliesShipmentCreated() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope command = createCommand(orderId);

        handler.handle(command);

        EventEnvelope reply = single(replies(orderId));
        assertThat(reply.eventType()).isEqualTo("ShipmentCreated");
        assertThat(reply.sagaId()).isEqualTo(command.sagaId());
        assertThat(reply.payload().get("shipmentId").asText()).isNotBlank();
        assertThat(shipmentStatus(orderId)).isEqualTo("CREATED");
    }

    @Test
    void duplicateDeliveryCreatesOneShipmentAndOneReply() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope command = createCommand(orderId);

        assertThat(handler.handle(command)).isTrue();
        assertThat(handler.handle(command)).isFalse();

        assertThat(replies(orderId)).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM shipments WHERE order_id = ?", Integer.class, orderId)).isEqualTo(1);
    }

    @Test
    void retriedCreateWithNewEventIdRepeatsTheSameShipment() {
        UUID orderId = UUID.randomUUID();

        handler.handle(createCommand(orderId));
        handler.handle(createCommand(orderId));

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType).containsExactly("ShipmentCreated", "ShipmentCreated");
        assertThat(replies.get(1).payload()).isEqualTo(replies.get(0).payload());
    }

    @Test
    void restrictedSkuFailsFulfillmentAndRetryRepeatsTheAnswer() {
        UUID orderId = UUID.randomUUID();

        handler.handle(createCommand(orderId, "c1", Map.of("SKU-0001", 1, RESTRICTED_SKU, 1)));
        handler.handle(createCommand(orderId, "c1", Map.of("SKU-0001", 1, RESTRICTED_SKU, 1)));

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType).containsExactly("FulfillmentFailed", "FulfillmentFailed");
        assertThat(reason(replies.get(0))).isEqualTo("RESTRICTED_SKU");
        assertThat(shipmentStatus(orderId)).isEqualTo("REJECTED");
    }

    @Test
    void invalidCreateFailsFulfillment() {
        UUID orderId = UUID.randomUUID();

        handler.handle(message("CreateShipment", orderId, objectMapper.createObjectNode().put("customerId", "c1")));

        assertThat(reason(single(replies(orderId)))).isEqualTo("INVALID_REQUEST");
    }

    @Test
    void cancelCancelsTheShipmentAndSecondCancelIsNoOp() {
        UUID orderId = UUID.randomUUID();
        handler.handle(createCommand(orderId));

        handler.handle(cancelCommand(orderId));
        handler.handle(cancelCommand(orderId));

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType)
                .containsExactly("ShipmentCreated", "ShipmentCancelled", "ShipmentCancelled");
        assertThat(replies.get(1).payload().get("cancelled").asBoolean()).isTrue();
        assertThat(replies.get(2).payload()).isEqualTo(replies.get(1).payload());
        assertThat(shipmentStatus(orderId)).isEqualTo("CANCELLED");
    }

    @Test
    void cancelBeforeCreateIsNoOpAndBlocksLateCreate() {
        UUID orderId = UUID.randomUUID();

        // CreateShipment timed out in a retry topic; the saga compensated before it arrived.
        handler.handle(cancelCommand(orderId));
        handler.handle(createCommand(orderId));

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType).containsExactly("ShipmentCancelled", "FulfillmentFailed");
        assertThat(replies.get(0).payload().get("cancelled").asBoolean()).isFalse();
        assertThat(reason(replies.get(1))).isEqualTo("ORDER_CANCELLED");
        assertThat(jdbc.queryForObject("SELECT shipment_id IS NULL FROM shipments WHERE order_id = ?", Boolean.class, orderId)).isTrue();
    }

    @Test
    void orderCompletedDispatchesTheShipmentAndNotifiesOnce() {
        UUID orderId = UUID.randomUUID();
        handler.handle(createCommand(orderId));
        EventEnvelope completed = orderCompleted(orderId, "alice");
        double shippedBefore = sent("ORDER_SHIPPED");

        assertThat(handler.handle(completed)).isTrue();
        assertThat(handler.handle(completed)).isFalse();
        handler.handle(orderCompleted(orderId, "alice"));

        assertThat(shipmentStatus(orderId)).isEqualTo("SHIPPED");
        List<EventEnvelope> events = replies(orderId);
        assertThat(events).extracting(EventEnvelope::eventType).containsExactly("ShipmentCreated", "ShipmentShipped");
        assertThat(events.get(1).payload().get("trackingNumber").asText()).startsWith("TRK");
        assertThat(notifications(orderId)).containsExactlyInAnyOrder("ORDER_CONFIRMED", "ORDER_SHIPPED");
        assertThat(sent("ORDER_SHIPPED") - shippedBefore).isEqualTo(1.0);
        assertThat(jdbc.queryForObject("SELECT customer_id FROM notifications WHERE order_id = ? AND type = 'ORDER_SHIPPED'",
                String.class, orderId)).isEqualTo("alice");
    }

    @Test
    void shippedShipmentCannotBeCancelled() {
        UUID orderId = UUID.randomUUID();
        handler.handle(createCommand(orderId));
        handler.handle(orderCompleted(orderId, "bob"));

        handler.handle(cancelCommand(orderId));

        EventEnvelope reply = replies(orderId).getLast();
        assertThat(reply.eventType()).isEqualTo("ShipmentCancelFailed");
        assertThat(reason(reply)).isEqualTo("ALREADY_SHIPPED");
        assertThat(shipmentStatus(orderId)).isEqualTo("SHIPPED");
    }

    @Test
    void orderCancelledNotifiesOnceEvenWithoutAShipment() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope cancelled = orderCancelled(orderId, "CARD_DECLINED");

        assertThat(handler.handle(cancelled)).isTrue();
        assertThat(handler.handle(cancelled)).isFalse();
        handler.handle(orderCancelled(orderId, "CARD_DECLINED"));

        assertThat(notifications(orderId)).containsExactly("ORDER_CANCELLED");
        assertThat(shipmentStatus(orderId)).isNull();
        assertThat(replies(orderId)).isEmpty();
    }

    @Test
    void concurrentCreateCommandsRecordOneShipment() throws Exception {
        UUID orderId = UUID.randomUUID();
        int deliveries = 8;
        List<EventEnvelope> commands = new ArrayList<>();
        for (int i = 0; i < deliveries; i++) {
            commands.add(createCommand(orderId));
        }
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Boolean>> tasks = commands.stream().<Callable<Boolean>>map(c -> () -> {
            start.await();
            return handler.handle(c);
        }).toList();

        List<EventEnvelope> losers = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(deliveries);
        try {
            List<Future<Boolean>> futures = tasks.stream().map(pool::submit).toList();
            start.countDown();
            for (int i = 0; i < deliveries; i++) {
                try {
                    futures.get(i).get();
                } catch (ExecutionException e) {
                    // Lost the race on the order_id key; the whole transaction rolled back.
                    assertThat(e.getCause()).isInstanceOf(DuplicateKeyException.class);
                    losers.add(commands.get(i));
                }
            }
        } finally {
            pool.shutdownNow();
        }
        // Kafka would redeliver the rolled-back commands; they now see the stored shipment.
        losers.forEach(handler::handle);

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).hasSize(deliveries).allMatch(r -> r.eventType().equals("ShipmentCreated"));
        assertThat(replies.stream().map(r -> r.payload().get("shipmentId").asText()).distinct()).hasSize(1);
    }

    private double sent(String type) {
        var counter = meterRegistry.find("notifications_sent_total").tag("type", type).counter();
        return counter == null ? 0 : counter.count();
    }

    private static EventEnvelope single(List<EventEnvelope> replies) {
        assertThat(replies).hasSize(1);
        return replies.getFirst();
    }

    private static String reason(EventEnvelope reply) {
        return reply.payload().get("reason").asText();
    }
}
