package com.nexus.inventory;

import com.nexus.inventory.messaging.InventoryCommandHandler;
import com.nexus.inventory.reservation.ReservationExpirySweeper;
import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reservation TTL (SPEC.md §7): the sweeper releases overdue holds, and the commands that can arrive
 * afterwards (release, commit, a late reserve) behave.
 */
class ReservationExpiryIT extends InventoryIntegrationTest {

    @Autowired
    InventoryCommandHandler handler;

    @Autowired
    ReservationExpirySweeper sweeper;

    @Autowired
    EnvelopeMapper envelopeMapper;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    void overdueHoldIsReleasedAndReportedToItsSaga() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        EventEnvelope reserve = reserveCommand(orderId, Map.of(sku, 4));
        handler.handle(reserve);
        double expiredBefore = counter("reservations_expired_total");

        makeOverdue(orderId);
        sweeper.sweep();

        assertThat(available(sku)).isEqualTo(10);
        assertThat(reserved(sku)).isZero();
        assertThat(statuses(orderId)).containsExactly("EXPIRED");
        EventEnvelope expired = replies(orderId).getLast();
        assertThat(expired.eventType()).isEqualTo("ReservationExpired");
        assertThat(expired.sagaId()).isEqualTo(reserve.sagaId());
        assertThat(expired.payload().get("items").get(0).get("quantity").asInt()).isEqualTo(4);
        assertThat(counter("reservations_expired_total") - expiredBefore).isEqualTo(1.0);
    }

    @Test
    void holdsWithinTheirTtlAndSettledHoldsAreLeftAlone() {
        String sku = newSku(10);
        UUID fresh = UUID.randomUUID();
        UUID committed = UUID.randomUUID();
        UUID released = UUID.randomUUID();
        handler.handle(reserveCommand(fresh, Map.of(sku, 1)));
        handler.handle(reserveCommand(committed, Map.of(sku, 1)));
        handler.handle(commitCommand(committed));
        handler.handle(reserveCommand(released, Map.of(sku, 1)));
        handler.handle(releaseCommand(released));
        makeOverdue(committed);
        makeOverdue(released);

        sweeper.sweep();

        assertThat(statuses(fresh)).containsExactly("HELD");
        assertThat(statuses(committed)).containsExactly("COMMITTED");
        assertThat(statuses(released)).containsExactly("RELEASED");
        assertThat(available(sku)).isEqualTo(8);
        assertThat(reserved(sku)).isEqualTo(1);
    }

    @Test
    void secondSweepAndLateReleaseAreNoOps() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 3)));
        makeOverdue(orderId);

        assertThat(sweeper.sweep()).isPositive();
        sweeper.sweep();
        handler.handle(releaseCommand(orderId));

        assertThat(available(sku)).isEqualTo(10);
        assertThat(replies(orderId).stream().filter(r -> r.eventType().equals("ReservationExpired"))).hasSize(1);
        assertThat(replies(orderId).getLast().payload().get("items")).isEmpty();
    }

    @Test
    void lateReserveAfterExpiryIsRejected() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 2)));
        makeOverdue(orderId);
        sweeper.sweep();

        handler.handle(reserveCommand(orderId, Map.of(sku, 2)));

        EventEnvelope reply = replies(orderId).getLast();
        assertThat(reply.eventType()).isEqualTo("InventoryRejected");
        assertThat(reply.payload().get("reason").asText()).isEqualTo("ALREADY_RELEASED");
        assertThat(available(sku)).isEqualTo(10);
    }

    @Test
    void commitAfterExpiryRetakesTheStock() {
        String sku = newSku(10);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 3)));
        makeOverdue(orderId);
        sweeper.sweep();

        handler.handle(commitCommand(orderId));

        assertThat(available(sku)).isEqualTo(7);
        assertThat(reserved(sku)).isZero();
        assertThat(statuses(orderId)).containsExactly("COMMITTED");
        EventEnvelope reply = replies(orderId).getLast();
        assertThat(reply.eventType()).isEqualTo("InventoryCommitted");
        assertThat(reply.payload().get("items")).hasSize(1);
        assertThat(reply.payload().has("shortfall")).isFalse();
    }

    @Test
    void commitAfterExpiryWithStockGoneReportsShortfall() {
        String sku = newSku(3);
        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 3)));
        makeOverdue(orderId);
        sweeper.sweep();
        handler.handle(reserveCommand(UUID.randomUUID(), Map.of(sku, 2)));
        double before = counter("inventory_commit_shortfall_total");

        handler.handle(commitCommand(orderId));

        assertThat(available(sku)).isEqualTo(1);
        assertThat(reserved(sku)).isEqualTo(2);
        assertThat(statuses(orderId)).containsExactly("EXPIRED");
        EventEnvelope reply = replies(orderId).getLast();
        assertThat(reply.payload().get("items")).isEmpty();
        assertThat(reply.payload().get("shortfall").get(0).get("quantity").asInt()).isEqualTo(3);
        assertThat(counter("inventory_commit_shortfall_total") - before).isEqualTo(1.0);
    }

    private void makeOverdue(UUID orderId) {
        jdbc.update("UPDATE reservations SET expires_at = now() - interval '1 second' WHERE order_id = ?", orderId);
    }

    private List<String> statuses(UUID orderId) {
        return jdbc.queryForList("SELECT DISTINCT status FROM reservations WHERE order_id = ?", String.class, orderId);
    }

    private double counter(String name) {
        var counter = meterRegistry.find(name).counter();
        return counter == null ? 0 : counter.count();
    }

    private List<EventEnvelope> replies(UUID orderId) {
        return jdbc.query("SELECT payload::text FROM outbox WHERE aggregate_id = ? ORDER BY created_at, payload->>'occurredAt'",
                (rs, i) -> envelopeMapper.fromJson(rs.getString(1)), orderId.toString());
    }
}
