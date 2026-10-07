package com.nexus.inventory.reservation;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class ReservationRepository {

    public enum Status { HELD, RELEASED, COMMITTED }

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

    public void insertHeld(UUID orderId, List<LineItem> items, Instant expiresAt) {
        jdbcTemplate.batchUpdate("""
                        INSERT INTO reservations (order_id, sku, quantity, status, expires_at)
                        VALUES (?, ?, ?, 'HELD', ?)
                        """,
                items, items.size(), (ps, item) -> {
                    ps.setObject(1, orderId);
                    ps.setString(2, item.sku());
                    ps.setInt(3, item.quantity());
                    ps.setTimestamp(4, Timestamp.from(expiresAt));
                });
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
