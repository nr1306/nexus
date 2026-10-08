package com.nexus.inventory.reservation;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Repository
public class ReservationRepository {

    public enum Status { HELD, RELEASED, COMMITTED, EXPIRED }

    /** Stock returned by an expired hold, and the saga that held it ({@code null} for pre-V15 rows). */
    public record Expired(UUID sagaId, List<LineItem> items) {
    }

    public record Reservation(String sku, int quantity, Status status, Instant expiresAt) {
    }

    private final JdbcTemplate jdbcTemplate;

    public ReservationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Reservation> findByOrder(UUID orderId) {
        return jdbcTemplate.query("""
                        SELECT sku, quantity, status, expires_at
                          FROM reservations
                         WHERE order_id = ?
                         ORDER BY sku
                        """,
                (rs, i) -> new Reservation(
                        rs.getString("sku"),
                        rs.getInt("quantity"),
                        Status.valueOf(rs.getString("status")),
                        rs.getTimestamp("expires_at").toInstant()),
                orderId);
    }

    public void insertHeld(UUID orderId, UUID sagaId, List<LineItem> items, Instant expiresAt) {
        jdbcTemplate.batchUpdate("""
                        INSERT INTO reservations (order_id, saga_id, sku, quantity, status, expires_at)
                        VALUES (?, ?, ?, ?, 'HELD', ?)
                        """,
                items, items.size(), (ps, item) -> {
                    ps.setObject(1, orderId);
                    ps.setObject(2, sagaId);
                    ps.setString(3, item.sku());
                    ps.setInt(4, item.quantity());
                    ps.setTimestamp(5, Timestamp.from(expiresAt));
                });
    }

    /** Orders holding at least one reservation past its expiry. */
    public List<UUID> findExpiredOrders(Instant now, int limit) {
        return jdbcTemplate.queryForList("""
                SELECT DISTINCT order_id FROM reservations
                 WHERE status = 'HELD' AND expires_at <= ?
                 LIMIT ?
                """, UUID.class, Timestamp.from(now), limit);
    }

    /**
     * Marks the order's overdue HELD reservations EXPIRED and returns them. Row locks serialize this with
     * a concurrent release or commit of the same order: whichever runs second finds nothing HELD.
     */
    public Expired expireHeld(UUID orderId, Instant now) {
        List<UUID> sagaIds = new ArrayList<>();
        List<LineItem> items = jdbcTemplate.query("""
                        UPDATE reservations
                           SET status = 'EXPIRED', released_at = ?
                         WHERE order_id = ? AND status = 'HELD' AND expires_at <= ?
                        RETURNING sku, quantity, saga_id
                        """,
                (rs, i) -> {
                    sagaIds.add(rs.getObject("saga_id", UUID.class));
                    return new LineItem(rs.getString("sku"), rs.getInt("quantity"));
                },
                Timestamp.from(now), orderId, Timestamp.from(now));
        return new Expired(sagaIds.stream().filter(Objects::nonNull).findFirst().orElse(null), items);
    }

    /** Marks the order's EXPIRED reservations COMMITTED (the stock was re-taken) and returns them. */
    public List<LineItem> commitExpired(UUID orderId, Instant committedAt) {
        return jdbcTemplate.query("""
                        UPDATE reservations
                           SET status = 'COMMITTED', committed_at = ?
                         WHERE order_id = ? AND status = 'EXPIRED'
                        RETURNING sku, quantity
                        """,
                (rs, i) -> new LineItem(rs.getString("sku"), rs.getInt("quantity")),
                Timestamp.from(committedAt), orderId);
    }

    /**
     * Marks the order's HELD reservations RELEASED and returns them. Row locks make concurrent
     * releases of the same order return each row at most once.
     */
    public List<LineItem> releaseHeld(UUID orderId, Instant releasedAt) {
        return jdbcTemplate.query("""
                        UPDATE reservations
                           SET status = 'RELEASED', released_at = ?
                         WHERE order_id = ? AND status = 'HELD'
                        RETURNING sku, quantity
                        """,
                (rs, i) -> new LineItem(rs.getString("sku"), rs.getInt("quantity")),
                Timestamp.from(releasedAt), orderId);
    }

    /** Records that the order was released; later reserves for it are rejected. */
    public void markReleased(UUID orderId, Instant releasedAt) {
        jdbcTemplate.update("""
                INSERT INTO released_orders (order_id, released_at) VALUES (?, ?)
                ON CONFLICT (order_id) DO NOTHING
                """, orderId, Timestamp.from(releasedAt));
    }

    public boolean isReleased(UUID orderId) {
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM released_orders WHERE order_id = ?)", Boolean.class, orderId));
    }

    /** Marks the order's HELD reservations COMMITTED and returns them. */
    public List<LineItem> commitHeld(UUID orderId, Instant committedAt) {
        return jdbcTemplate.query("""
                        UPDATE reservations
                           SET status = 'COMMITTED', committed_at = ?
                         WHERE order_id = ? AND status = 'HELD'
                        RETURNING sku, quantity
                        """,
                (rs, i) -> new LineItem(rs.getString("sku"), rs.getInt("quantity")),
                Timestamp.from(committedAt), orderId);
    }
}
