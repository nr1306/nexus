package com.nexus.fulfillment.messaging;

import com.nexus.fulfillment.shipment.FulfillmentMessages;
import com.nexus.fulfillment.shipment.ShipmentService;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.idempotency.IdempotentConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * Applies one fulfillment command or order event at most once (CLAUDE.md rule 2).
 */
@Component
public class FulfillmentHandler {

    public static final String CONSUMER_GROUP = "fulfillment-svc";

    private static final Logger log = LoggerFactory.getLogger(FulfillmentHandler.class);

    private final IdempotentConsumer idempotentConsumer;
    private final ShipmentService shipmentService;

    public FulfillmentHandler(IdempotentConsumer idempotentConsumer, ShipmentService shipmentService) {
        this.idempotentConsumer = idempotentConsumer;
        this.shipmentService = shipmentService;
    }

    /** @return {@code false} if the message was a duplicate and was skipped */
    public boolean handle(EventEnvelope message) {
        try (var orderId = MDC.putCloseable("orderId", message.orderId().toString());
             var sagaId = MDC.putCloseable("sagaId", message.sagaId().toString());
             var eventId = MDC.putCloseable("eventId", message.eventId().toString());
             var eventType = MDC.putCloseable("eventType", message.eventType())) {
            boolean processed = idempotentConsumer.handle(CONSUMER_GROUP, message, this::apply);
            if (!processed) {
                log.info("Skipped duplicate message");
            }
            return processed;
        }
    }

    private void apply(EventEnvelope message) {
        switch (message.eventType()) {
            case FulfillmentMessages.CREATE_SHIPMENT -> shipmentService.create(message);
            case FulfillmentMessages.CANCEL_SHIPMENT -> shipmentService.cancel(message);
            case FulfillmentMessages.ORDER_COMPLETED -> shipmentService.onOrderCompleted(message);
            case FulfillmentMessages.ORDER_CANCELLED -> shipmentService.onOrderCancelled(message);
            // OrderCreated, OrderNeedsAttention: nothing to do.
            default -> log.debug("Ignoring {}", message.eventType());
        }
    }
}
