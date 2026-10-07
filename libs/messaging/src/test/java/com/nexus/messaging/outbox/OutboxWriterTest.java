package com.nexus.messaging.outbox;

import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.support.PostgresTestSupport;
import com.nexus.messaging.support.TestEnvelopes;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxWriterTest extends PostgresTestSupport {

    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private final EnvelopeMapper envelopeMapper = new EnvelopeMapper(TestEnvelopes.OBJECT_MAPPER);
    private final OutboxWriter writer = new OutboxWriter(JDBC, envelopeMapper, () -> Optional.of(TRACEPARENT));

    @Test
    void appendsEnvelopeInsideTransaction() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope envelope = TestEnvelopes.reserveInventory(orderId);

        TX.executeWithoutResult(status -> writer.append("inventory.commands", "order", envelope));

        Map<String, Object> row = JDBC.queryForMap("SELECT * FROM outbox WHERE id = ?", envelope.eventId());
        assertThat(row)
                .containsEntry("aggregate_type", "order")
                .containsEntry("aggregate_id", orderId.toString())
                .containsEntry("event_type", "ReserveInventory")
                .containsEntry("topic", "inventory.commands")
                .containsEntry("traceparent", TRACEPARENT);
        assertThat(envelopeMapper.fromJson(row.get("payload").toString())).isEqualTo(envelope);
    }

    @Test
    void refusesToWriteOutsideTransaction() {
        EventEnvelope envelope = TestEnvelopes.reserveInventory(UUID.randomUUID());

        assertThatThrownBy(() -> writer.append("inventory.commands", "order", envelope))
                .isInstanceOf(IllegalStateException.class);
        assertThat(outboxCount()).isZero();
    }

    @Test
    void rolledBackTransactionLeavesNoOutboxRow() {
        TX.executeWithoutResult(status -> {
            writer.append("inventory.commands", "order", TestEnvelopes.reserveInventory(UUID.randomUUID()));
            status.setRollbackOnly();
        });

        assertThat(outboxCount()).isZero();
    }

    @Test
    void storesNullTraceparentWhenNoSpanIsActive() {
        OutboxWriter untraced = new OutboxWriter(JDBC, envelopeMapper, TraceparentSource.none());
        EventEnvelope envelope = TestEnvelopes.reserveInventory(UUID.randomUUID());

        TX.executeWithoutResult(status -> untraced.append("inventory.commands", "order", envelope));

        assertThat(JDBC.queryForObject("SELECT traceparent FROM outbox WHERE id = ?", String.class, envelope.eventId()))
                .isNull();
    }

    private int outboxCount() {
        return JDBC.queryForObject("SELECT count(*) FROM outbox", Integer.class);
    }
}
