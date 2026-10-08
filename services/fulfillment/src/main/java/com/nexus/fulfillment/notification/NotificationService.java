package com.nexus.fulfillment.notification;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Sends each (order, type) notification at most once. The row is inserted first; only the transaction
 * that inserts it sends. Runs inside the caller's idempotent-consumer transaction.
 */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class NotificationService {

    static final String CHANNEL = "EMAIL";

    private final JdbcTemplate jdbcTemplate;
    private final NotificationSender sender;
    private final MeterRegistry meterRegistry;

    public NotificationService(JdbcTemplate jdbcTemplate, NotificationSender sender, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.sender = sender;
        this.meterRegistry = meterRegistry;
    }

    /** @return {@code false} if this notification was already sent for the order */
    public boolean notify(UUID orderId, NotificationType type, String customerId, String body) {
        UUID id = notificationId(orderId, type);
        int inserted = jdbcTemplate.update("""
                        INSERT INTO notifications (id, order_id, type, customer_id, channel, body)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT (order_id, type) DO NOTHING
                        """, id, orderId, type.name(), customerId, CHANNEL, body);
        if (inserted == 0) {
            return false;
        }
        sender.send(id, customerId, type, body);
        meterRegistry.counter("notifications_sent_total", "type", type.name()).increment();
        return true;
    }

    static UUID notificationId(UUID orderId, NotificationType type) {
        return UUID.nameUUIDFromBytes((orderId + ":" + type).getBytes(StandardCharsets.UTF_8));
    }
}
