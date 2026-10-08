package com.nexus.order.saga;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class SagaRepository {

    private static final String COLUMNS = """
            saga_id, order_id, state, step, step_deadline, step_attempts, compensations, failure_reason, created_at
            """;

    private static final RowMapper<SagaInstance> MAPPER = (rs, i) -> {
        String step = rs.getString("step");
        Timestamp deadline = rs.getTimestamp("step_deadline");
        String[] compensations = (String[]) rs.getArray("compensations").getArray();
        return new SagaInstance(
                rs.getObject("saga_id", UUID.class),
                rs.getObject("order_id", UUID.class),
                SagaState.valueOf(rs.getString("state")),
                step == null ? null : SagaStep.valueOf(step),
                deadline == null ? null : deadline.toInstant(),
                rs.getInt("step_attempts"),
                Arrays.stream(compensations).map(SagaStep::valueOf).toList(),
                rs.getString("failure_reason"),
                rs.getTimestamp("created_at").toInstant());
    };

    private final JdbcTemplate jdbcTemplate;

    public SagaRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(SagaInstance saga) {
        jdbcTemplate.update(con -> {
            var ps = con.prepareStatement("""
                    INSERT INTO saga_instances (saga_id, order_id, state, step, step_deadline, step_attempts,
                                                compensations, failure_reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """);
            ps.setObject(1, saga.sagaId());
            ps.setObject(2, saga.orderId());
            ps.setString(3, saga.state().name());
            ps.setString(4, saga.step() == null ? null : saga.step().name());
            ps.setTimestamp(5, timestamp(saga.stepDeadline()));
            ps.setInt(6, saga.stepAttempts());
            ps.setArray(7, steps(con, saga.compensations()));
            ps.setString(8, saga.failureReason());
            ps.setTimestamp(9, timestamp(saga.createdAt()));
            return ps;
        });
    }

    public void update(SagaInstance saga) {
        jdbcTemplate.update(con -> {
            var ps = con.prepareStatement("""
                    UPDATE saga_instances
                       SET state = ?, step = ?, step_deadline = ?, step_attempts = ?, compensations = ?,
                           failure_reason = ?, updated_at = now()
                     WHERE saga_id = ?
                    """);
            ps.setString(1, saga.state().name());
            ps.setString(2, saga.step() == null ? null : saga.step().name());
            ps.setTimestamp(3, timestamp(saga.stepDeadline()));
            ps.setInt(4, saga.stepAttempts());
            ps.setArray(5, steps(con, saga.compensations()));
            ps.setString(6, saga.failureReason());
            ps.setObject(7, saga.sagaId());
            return ps;
        });
    }

    /** Locks the order's saga row for the rest of the transaction (serializes replies and timeouts). */
    public Optional<SagaInstance> lockByOrderId(UUID orderId) {
        return jdbcTemplate.query("SELECT " + COLUMNS + " FROM saga_instances WHERE order_id = ? FOR UPDATE",
                MAPPER, orderId).stream().findFirst();
    }

    /** Reads the saga without locking. */
    public Optional<SagaInstance> findByOrderId(UUID orderId) {
        return jdbcTemplate.query("SELECT " + COLUMNS + " FROM saga_instances WHERE order_id = ?", MAPPER, orderId)
                .stream().findFirst();
    }

    /** Ids of sagas currently awaiting {@code step}, oldest deadline first. */
    public List<UUID> findAwaiting(SagaStep step, int limit) {
        return jdbcTemplate.queryForList("""
                SELECT order_id FROM saga_instances
                 WHERE step = ?
                 ORDER BY step_deadline
                 LIMIT ?
                """, UUID.class, step.name(), limit);
    }

    /** Ids of sagas whose awaited reply is overdue. */
    public List<UUID> findOverdue(Instant now, int limit) {
        return jdbcTemplate.queryForList("""
                SELECT order_id FROM saga_instances
                 WHERE step IS NOT NULL AND step_deadline < ?
                 ORDER BY step_deadline
                 LIMIT ?
                """, UUID.class, Timestamp.from(now), limit);
    }

    /**
     * Locks the saga if it is still overdue and no other transaction holds it (another pod's sweeper or
     * a reply being processed).
     */
    public Optional<SagaInstance> lockIfOverdue(UUID orderId, Instant now) {
        return jdbcTemplate.query("SELECT " + COLUMNS + """
                         FROM saga_instances
                        WHERE order_id = ? AND step IS NOT NULL AND step_deadline < ?
                        FOR UPDATE SKIP LOCKED
                        """,
                MAPPER, orderId, Timestamp.from(now)).stream().findFirst();
    }

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Array steps(Connection con, List<SagaStep> steps) throws SQLException {
        return con.createArrayOf("text", steps.stream().map(Enum::name).toArray());
    }
}
