package com.nexus.fraud.decision;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

@Repository
public class DecisionRepository {

    private final JdbcTemplate jdbcTemplate;

    public DecisionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<FraudDecision> find(UUID orderId) {
        return jdbcTemplate.query("SELECT decision, reasons FROM decisions WHERE order_id = ?",
                (rs, i) -> new FraudDecision(orderId, "APPROVE".equals(rs.getString("decision")),
                        Arrays.asList((String[]) rs.getArray("reasons").getArray())),
                orderId).stream().findFirst();
    }

    /** Orders this customer placed since {@code since}, excluding {@code orderId}. */
    public int countRecent(String customerId, Instant since, UUID orderId) {
        return jdbcTemplate.queryForObject("""
                SELECT count(*) FROM decisions
                 WHERE customer_id = ? AND evaluated_at >= ? AND order_id <> ?
                """, Integer.class, customerId, Timestamp.from(since), orderId);
    }

    /** @return {@code false} if a decision for the order already exists (a concurrent evaluation won) */
    public boolean insert(EvaluationRequest request, FraudDecision decision, Instant evaluatedAt) {
        return jdbcTemplate.update(con -> {
            var ps = con.prepareStatement("""
                    INSERT INTO decisions (order_id, saga_id, customer_id, amount_cents, currency, payment_method,
                                           decision, reasons, evaluated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (order_id) DO NOTHING
                    """);
            ps.setObject(1, request.orderId());
            ps.setObject(2, request.sagaId());
            ps.setString(3, request.customerId());
            ps.setLong(4, request.amountCents());
            ps.setString(5, request.currency());
            ps.setString(6, request.paymentMethod());
            ps.setString(7, decision.approved() ? "APPROVE" : "REJECT");
            ps.setArray(8, con.createArrayOf("text", decision.reasons().toArray()));
            ps.setTimestamp(9, Timestamp.from(evaluatedAt));
            return ps;
        }) == 1;
    }

    public boolean isBlocked(String kind, String value) {
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM blocklist WHERE kind = ? AND value = ?)", Boolean.class, kind, value));
    }
}
