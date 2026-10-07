package com.nexus.order.order;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class OrderRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(OrderDetails order) {
        jdbcTemplate.update("""
                        INSERT INTO orders (id, customer_id, total_cents, currency, payment_method)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                order.orderId(), order.customerId(), order.totalCents(), order.currency(), order.paymentMethod());
        jdbcTemplate.batchUpdate("""
                        INSERT INTO order_items (order_id, sku, quantity, unit_price_cents)
                        VALUES (?, ?, ?, ?)
                        """,
                order.items(), order.items().size(), (ps, item) -> {
                    ps.setObject(1, order.orderId());
                    ps.setString(2, item.sku());
                    ps.setInt(3, item.quantity());
                    ps.setLong(4, item.unitPriceCents());
                });
    }

    public Optional<OrderDetails> findDetails(UUID orderId) {
        return jdbcTemplate.query("""
                        SELECT id, customer_id, total_cents, currency, payment_method FROM orders WHERE id = ?
                        """,
                (rs, i) -> new OrderDetails(
                        rs.getObject("id", UUID.class),
                        rs.getString("customer_id"),
                        items(orderId),
                        rs.getLong("total_cents"),
                        rs.getString("currency"),
                        rs.getString("payment_method")),
                orderId).stream().findFirst();
    }

    public Optional<OrderView> findView(UUID orderId) {
        return jdbcTemplate.query("""
                        SELECT o.id, o.customer_id, o.total_cents, o.currency, o.created_at,
                               s.saga_id, s.state, s.step, s.failure_reason, s.updated_at
                          FROM orders o
                          JOIN saga_instances s ON s.order_id = o.id
                         WHERE o.id = ?
                        """,
                (rs, i) -> new OrderView(
                        rs.getObject("id", UUID.class),
                        rs.getObject("saga_id", UUID.class),
                        rs.getString("state"),
                        rs.getString("step"),
                        rs.getString("failure_reason"),
                        rs.getString("customer_id"),
                        items(orderId),
                        rs.getLong("total_cents"),
                        rs.getString("currency"),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getTimestamp("updated_at").toInstant()),
                orderId).stream().findFirst();
    }

    private List<OrderView.Item> items(UUID orderId) {
        return jdbcTemplate.query("""
                        SELECT sku, quantity, unit_price_cents FROM order_items WHERE order_id = ? ORDER BY sku
                        """,
                (rs, i) -> new OrderView.Item(rs.getString("sku"), rs.getInt("quantity"), rs.getLong("unit_price_cents")),
                orderId);
    }
}
