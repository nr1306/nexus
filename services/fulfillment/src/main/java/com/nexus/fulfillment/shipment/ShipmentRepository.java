package com.nexus.fulfillment.shipment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.fulfillment.shipment.Shipment.Status;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ShipmentRepository {

    private static final TypeReference<List<FulfillmentMessages.Item>> ITEMS = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final RowMapper<Shipment> rowMapper;

    public ShipmentRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.rowMapper = (rs, i) -> new Shipment(
                rs.getObject("order_id", UUID.class),
                rs.getObject("shipment_id", UUID.class),
                Status.valueOf(rs.getString("status")),
                rs.getString("customer_id"),
                readItems(rs.getString("items")),
                rs.getString("failure_reason"),
                rs.getString("tracking_number"));
    }

    /** The order's row, locked until the transaction ends so create, cancel and dispatch never interleave. */
    public Optional<Shipment> lock(UUID orderId) {
        return jdbcTemplate.query("""
                        SELECT order_id, shipment_id, status, customer_id, items::text AS items, failure_reason, tracking_number
                          FROM shipments
                         WHERE order_id = ?
                           FOR UPDATE
                        """, rowMapper, orderId).stream().findFirst();
    }

    /**
     * Inserts the order's row. A concurrent insert for the same order fails on the primary key, rolling
     * back that transaction; its redelivery then finds this row.
     */
    public void insert(Shipment shipment) {
        jdbcTemplate.update("""
                        INSERT INTO shipments (order_id, shipment_id, status, customer_id, items, failure_reason, tracking_number)
                        VALUES (?, ?, ?, ?, ?::jsonb, ?, ?)
                        """,
                shipment.orderId(), shipment.shipmentId(), shipment.status().name(), shipment.customerId(),
                writeItems(shipment.items()), shipment.failureReason(), shipment.trackingNumber());
    }

    public void updateStatus(UUID orderId, Status status, String trackingNumber) {
        jdbcTemplate.update("""
                        UPDATE shipments
                           SET status = ?, tracking_number = coalesce(?, tracking_number), updated_at = now()
                         WHERE order_id = ?
                        """, status.name(), trackingNumber, orderId);
    }

    /** The given SKUs that the carrier refuses to ship, sorted. */
    public List<String> restricted(Collection<String> skus) {
        return jdbcTemplate.queryForList("SELECT sku FROM restricted_skus WHERE sku = ANY (?) ORDER BY sku",
                String.class, (Object) skus.toArray(new String[0]));
    }

    private List<FulfillmentMessages.Item> readItems(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, ITEMS);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupt shipments.items", e);
        }
    }

    private String writeItems(List<FulfillmentMessages.Item> items) {
        if (items == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(items);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
