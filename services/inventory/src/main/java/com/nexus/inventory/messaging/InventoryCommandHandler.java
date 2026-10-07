package com.nexus.inventory.messaging;

import com.nexus.inventory.reservation.InventoryMessages;
import com.nexus.inventory.reservation.ReservationService;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.idempotency.IdempotentConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * Applies one inventory command at most once (CLAUDE.md rule 2).
 */
@Component
public class InventoryCommandHandler {

    public static final String CONSUMER_GROUP = "inventory-svc";

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandHandler.class);

    private final IdempotentConsumer idempotentConsumer;
    private final ReservationService reservationService;

    public InventoryCommandHandler(IdempotentConsumer idempotentConsumer, ReservationService reservationService) {
        this.idempotentConsumer = idempotentConsumer;
        this.reservationService = reservationService;
    }

    /** @return {@code false} if the command was a duplicate and was skipped */
    public boolean handle(EventEnvelope command) {
        try (var orderId = MDC.putCloseable("orderId", command.orderId().toString());
             var sagaId = MDC.putCloseable("sagaId", command.sagaId().toString());
             var eventId = MDC.putCloseable("eventId", command.eventId().toString());
             var eventType = MDC.putCloseable("eventType", command.eventType())) {
            boolean processed = idempotentConsumer.handle(CONSUMER_GROUP, command, this::apply);
            if (!processed) {
                log.info("Skipped duplicate command");
            }
            return processed;
        }
    }

    private void apply(EventEnvelope command) {
        switch (command.eventType()) {
            case InventoryMessages.RESERVE_INVENTORY -> reservationService.reserve(command);
            case InventoryMessages.RELEASE_INVENTORY -> reservationService.release(command);
            case InventoryMessages.COMMIT_INVENTORY -> reservationService.commit(command);
            default -> log.warn("Ignoring unknown command type");
        }
    }
}
