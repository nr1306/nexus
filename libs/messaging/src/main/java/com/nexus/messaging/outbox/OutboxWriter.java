package com.nexus.messaging.outbox;

import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Appends outgoing messages to the {@code outbox} table. Must run inside the same transaction as the
 * state change it announces (CLAUDE.md rule 1); Debezium publishes the row after commit.
 */
public class OutboxWriter {

    private static final String INSERT = """
            INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload, traceparent)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?)
            """;

    private final JdbcTemplate jdbcTemplate;
    private final EnvelopeMapper envelopeMapper;
    private final TraceparentSource traceparentSource;

    public OutboxWriter(JdbcTemplate jdbcTemplate, EnvelopeMapper envelopeMapper, TraceparentSource traceparentSource) {
        this.jdbcTemplate = jdbcTemplate;
        this.envelopeMapper = envelopeMapper;
        this.traceparentSource = traceparentSource;
    }

    /**
     * @param topic         destination topic, e.g. {@code inventory.commands}
     * @param aggregateType the aggregate the message concerns, e.g. {@code order}
     */
    public void append(String topic, String aggregateType, EventEnvelope envelope) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException(
                    "OutboxWriter.append must run inside the transaction that changes state (eventId="
                            + envelope.eventId() + ")");
        }
        jdbcTemplate.update(INSERT,
                envelope.eventId(),
                aggregateType,
                envelope.orderId().toString(),
                envelope.eventType(),
                topic,
                envelopeMapper.toJson(envelope),
                traceparentSource.current().orElse(null));
    }
}
