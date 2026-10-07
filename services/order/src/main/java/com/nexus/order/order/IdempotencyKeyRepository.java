package com.nexus.order.order;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class IdempotencyKeyRepository {

    public record StoredKey(String requestHash, UUID orderId) {
    }

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyKeyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Claims the key for a new order. If another transaction holds the same key, this waits for it to
     * finish; it returns {@code false} if that transaction committed.
     */
    public boolean claim(String key, String requestHash, UUID orderId) {
        return jdbcTemplate.update("""
                INSERT INTO idempotency_keys (key, request_hash, order_id)
                VALUES (?, ?, ?)
                ON CONFLICT (key) DO NOTHING
                """, key, requestHash, orderId) == 1;
    }

    public StoredKey find(String key) {
        return jdbcTemplate.queryForObject(
                "SELECT request_hash, order_id FROM idempotency_keys WHERE key = ?",
                (rs, i) -> new StoredKey(rs.getString("request_hash"), rs.getObject("order_id", UUID.class)),
                key);
    }
}
