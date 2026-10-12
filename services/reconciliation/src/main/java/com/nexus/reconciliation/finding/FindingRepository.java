package com.nexus.reconciliation.finding;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
class FindingRepository {

    /** An order to act on: the projection's order and saga ids. */
    record Target(UUID orderId, UUID sagaId) {
    }

    private static final RowMapper<Target> TARGET =
            (rs, i) -> new Target(rs.getObject("order_id", UUID.class), rs.getObject("saga_id", UUID.class));

    private static final RowMapper<Finding> FINDING = (rs, i) -> new Finding(
            rs.getObject("order_id", UUID.class),
            FindingType.valueOf(rs.getString("finding_type")),
            rs.getObject("saga_id", UUID.class),
            instant(rs.getTimestamp("detected_at")),
            instant(rs.getTimestamp("resolved_at")),
            rs.getString("repair_command"),
            rs.getInt("repair_attempts"),
            instant(rs.getTimestamp("last_repair_at")));

    private final NamedParameterJdbcTemplate jdbc;

    FindingRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Settled orders that violate {@code type} and have no finding of that type yet. */
    List<Target> findNewViolations(FindingType type, Instant settledBefore, Instant stuckBefore, int limit) {
        return jdbc.query("""
                SELECT p.order_id, p.saga_id FROM order_projection p
                WHERE p.last_event_at < :settledBefore AND (%s)
                  AND NOT EXISTS (SELECT 1 FROM findings f WHERE f.order_id = p.order_id AND f.finding_type = :type)
                ORDER BY p.last_event_at
                LIMIT :limit
                """.formatted(type.violation), params(type, stuckBefore)
                .addValue("settledBefore", Timestamp.from(settledBefore))
                .addValue("limit", limit), TARGET);
    }

    /** @return {@code true} if this call created the finding */
    boolean insert(Target target, FindingType type, Instant now) {
        return jdbc.update("""
                INSERT INTO findings (order_id, finding_type, saga_id, detected_at, repair_command)
                VALUES (:orderId, :type, :sagaId, :now, :repair)
                ON CONFLICT DO NOTHING
                """, new MapSqlParameterSource()
                .addValue("orderId", target.orderId())
                .addValue("type", type.name())
                .addValue("sagaId", target.sagaId())
                .addValue("now", Timestamp.from(now))
                .addValue("repair", type.repairCommand)) == 1;
    }

    void recordRepairSent(UUID orderId, FindingType type, Instant now) {
        jdbc.update("""
                UPDATE findings SET repair_attempts = repair_attempts + 1, last_repair_at = :now
                WHERE order_id = :orderId AND finding_type = :type
                """, new MapSqlParameterSource()
                .addValue("orderId", orderId).addValue("type", type.name()).addValue("now", Timestamp.from(now)));
    }

    /** Marks open findings of {@code type} whose invariant now holds as resolved. @return how many */
    int resolveFixed(FindingType type, Instant now, Instant stuckBefore) {
        return jdbc.update("""
                UPDATE findings f SET resolved_at = :now
                FROM order_projection p
                WHERE f.order_id = p.order_id AND f.finding_type = :type AND f.resolved_at IS NULL
                  AND NOT coalesce((%s), false)
                """.formatted(type.violation), params(type, stuckBefore).addValue("now", Timestamp.from(now)));
    }

    /** Open, repairable findings whose last repair is older than {@code retryBefore}. */
    List<Target> findRepairsToRetry(FindingType type, Instant retryBefore, int maxAttempts, int limit) {
        return jdbc.query("""
                SELECT order_id, saga_id FROM findings
                WHERE finding_type = :type AND resolved_at IS NULL AND repair_command IS NOT NULL
                  AND repair_attempts < :max AND last_repair_at < :retryBefore
                ORDER BY last_repair_at
                LIMIT :limit
                """, new MapSqlParameterSource()
                .addValue("type", type.name())
                .addValue("max", maxAttempts)
                .addValue("retryBefore", Timestamp.from(retryBefore))
                .addValue("limit", limit), TARGET);
    }

    long countOpen() {
        Long n = jdbc.queryForObject("SELECT count(*) FROM findings WHERE resolved_at IS NULL", new MapSqlParameterSource(), Long.class);
        return n == null ? 0 : n;
    }

    List<Finding> list(boolean openOnly, int limit) {
        return jdbc.query("SELECT * FROM findings " + (openOnly ? "WHERE resolved_at IS NULL " : "")
                + "ORDER BY detected_at DESC LIMIT :limit", new MapSqlParameterSource("limit", limit), FINDING);
    }

    private static MapSqlParameterSource params(FindingType type, Instant stuckBefore) {
        return new MapSqlParameterSource()
                .addValue("type", type.name())
                .addValue("stuckBefore", Timestamp.from(stuckBefore));
    }

    private static Instant instant(Timestamp t) {
        return t == null ? null : t.toInstant();
    }
}
