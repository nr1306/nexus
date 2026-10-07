package com.nexus.payment.payment;

import com.nexus.payment.payment.PaymentRecord.Operation;
import com.nexus.payment.payment.PaymentRecord.Status;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public class PaymentRepository {

    private final JdbcTemplate jdbcTemplate;

    public PaymentRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<PaymentRecord> find(UUID orderId, Operation operation) {
        return jdbcTemplate.query("""
                        SELECT order_id, operation, status, amount_cents, currency, provider, provider_ref, failure_reason
                          FROM payments
                         WHERE order_id = ? AND operation = ?
                        """,
                (rs, i) -> new PaymentRecord(
                        rs.getObject("order_id", UUID.class),
                        Operation.valueOf(rs.getString("operation")),
                        Status.valueOf(rs.getString("status")),
                        rs.getObject("amount_cents", Long.class),
                        rs.getString("currency"),
                        rs.getString("provider"),
                        rs.getString("provider_ref"),
                        rs.getString("failure_reason")),
                orderId, operation.name()).stream().findFirst();
    }

    /**
     * Inserts the outcome. A concurrent insert for the same (order, operation) fails on the primary key,
     * rolling back that transaction; its redelivery then finds this row (CLAUDE.md rule 6).
     */
    public void insert(PaymentRecord record) {
        jdbcTemplate.update("""
                        INSERT INTO payments (order_id, operation, status, amount_cents, currency, provider, provider_ref, failure_reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                record.orderId(), record.operation().name(), record.status().name(), record.amountCents(),
                record.currency(), record.provider(), record.providerRef(), record.failureReason());
    }
}
