package com.nexus.inventory.stock;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class StockRepository {

    private final JdbcTemplate jdbcTemplate;

    public StockRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Moves {@code quantity} from available to reserved only if enough is available (CLAUDE.md rule 5).
     *
     * @return {@code false} if the SKU is unknown or has too little available stock
     */
    public boolean tryReserve(String sku, int quantity) {
        return jdbcTemplate.update("""
                UPDATE stock
                   SET available = available - ?, reserved = reserved + ?, updated_at = now()
                 WHERE sku = ? AND available >= ?
                """, quantity, quantity, sku, quantity) == 1;
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
    }

    /** Removes {@code quantity} from reserved: the stock has been sold. */
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

    public boolean exists(String sku) {
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM stock WHERE sku = ?)", Boolean.class, sku));
    }
}
