package com.nexus.reconciliation.projection;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Repository
class ProjectionRepository {

    private final JdbcTemplate jdbc;

    ProjectionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Records that {@code fact} happened at {@code occurredAt} (or only that an event arrived, if
     * {@code fact} is null). Keeps the latest timestamp per fact and the first saga id seen.
     */
    void record(UUID orderId, UUID sagaId, Fact fact, Instant occurredAt, Instant receivedAt) {
        if (fact == null) {
            jdbc.update("""
                    INSERT INTO order_projection (order_id, saga_id, last_event_at) VALUES (?, ?, ?)
                    ON CONFLICT (order_id) DO UPDATE SET
                        saga_id = coalesce(order_projection.saga_id, EXCLUDED.saga_id),
                        last_event_at = greatest(order_projection.last_event_at, EXCLUDED.last_event_at)
                    """, orderId, sagaId, Timestamp.from(receivedAt));
            return;
        }
        String c = fact.column; // enum constant, never user input
        jdbc.update("""
                INSERT INTO order_projection (order_id, saga_id, %1$s, last_event_at) VALUES (?, ?, ?, ?)
                ON CONFLICT (order_id) DO UPDATE SET
                    saga_id = coalesce(order_projection.saga_id, EXCLUDED.saga_id),
                    %1$s = greatest(order_projection.%1$s, EXCLUDED.%1$s),
                    last_event_at = greatest(order_projection.last_event_at, EXCLUDED.last_event_at)
                """.formatted(c), orderId, sagaId, Timestamp.from(occurredAt), Timestamp.from(receivedAt));
    }
}
