package com.nexus.fulfillment.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;

/** Simulated channel: no e-mail leaves the system, the message is logged (SPEC.md: no customer-facing UI). */
@Component
class LoggingNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(LoggingNotificationSender.class);

    @Override
    public void send(UUID notificationId, String customerId, NotificationType type, String body) {
        log.info("Notification {} to customer {} [{}]: {}", type, customerId, notificationId, body);
    }
}
