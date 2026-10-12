package com.nexus.inventory.stock;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/** Every change to {@code available} evicts the SKU from the read cache once the transaction commits. */
@Repository
public class StockRepository {

    private final JdbcTemplate jdbcTemplate;
    private final StockCache cache;

    public StockRepository(JdbcTemplate jdbcTemplate, StockCache cache) {
        this.jdbcTemplate = jdbcTemplate;
        this.cache = cache;
    }

    /**
     * Moves {@code quantity} from available to reserved only if enough is available (CLAUDE.md rule 5).
     *
     * @return {@code false} if the SKU is unknown or has too little available stock
     */
    public boolean tryReserve(String sku, int quantity) {
        boolean reserved = jdbcTemplate.update("""
                UPDATE stock
                   SET available = available - ?, reserved = reserved + ?, updated_at = now()
                 WHERE sku = ? AND available >= ?
                """, quantity, quantity, sku, quantity) == 1;
        if (reserved) {
            cache.evictAfterCommit(sku);
        }
        return reserved;
    }

    /** Moves {@code quantity} from reserved back to available. */
    public void unreserve(String sku, int quantity) {
        int updated = jdbcTemplate.update("""
                UPDATE stock
                   SET available = available + ?, reserved = reserved - ?, updated_at = now()
                 WHERE sku = ?
                """, quantity, quantity, sku);
        if (updated != 1) {
            throw new IllegalStateException("Cannot unreserve unknown SKU " + sku);
        }
        cache.evictAfterCommit(sku);
    }

    /** Removes {@code quantity} from reserved: the stock has been sold ({@code available} is unchanged). */
    public void commit(String sku, int quantity) {
        int updated = jdbcTemplate.update("""
                UPDATE stock
                   SET reserved = reserved - ?, updated_at = now()
                 WHERE sku = ?
                """, quantity, sku);
        if (updated != 1) {
            throw new IllegalStateException("Cannot commit unknown SKU " + sku);
        }
    }

    /** Straight from Postgres; empty for an unknown SKU. */
    public Optional<Integer> available(String sku) {
        return jdbcTemplate.query("SELECT available FROM stock WHERE sku = ?", (rs, i) -> rs.getInt(1), sku)
                .stream().findFirst();
    }

    public boolean exists(String sku) {
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM stock WHERE sku = ?)", Boolean.class, sku));
    }
}
