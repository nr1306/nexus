package com.nexus.fulfillment.notification;

import java.util.UUID;

/**
 * Port to the channel that delivers customer messages. {@code notificationId} is stable per
 * (order, type), so a real provider can use it as an idempotency key: a redelivery after a rolled-back
 * transaction then doesn't reach the customer twice.
 */
public interface NotificationSender {

    void send(UUID notificationId, String customerId, NotificationType type, String body);
}
