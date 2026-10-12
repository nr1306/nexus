package com.nexus.reconciliation.messaging;

import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.idempotency.IdempotentConsumer;
import com.nexus.reconciliation.projection.ProjectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * Applies one event to the order projection at most once (CLAUDE.md rule 2). The projection is
 * idempotent by construction too (ADR 0009), but dedupe keeps {@code last_event_at} honest.
 */
@Component
public class ReconciliationHandler {

    public static final String CONSUMER_GROUP = "reconciliation";

    private static final Logger log = LoggerFactory.getLogger(ReconciliationHandler.class);

    private final IdempotentConsumer idempotentConsumer;
    private final ProjectionService projection;

    public ReconciliationHandler(IdempotentConsumer idempotentConsumer, ProjectionService projection) {
        this.idempotentConsumer = idempotentConsumer;
        this.projection = projection;
    }

    /** @return {@code false} if the event was a duplicate and was skipped */
    public boolean handle(EventEnvelope event) {
        try (var orderId = MDC.putCloseable("orderId", event.orderId().toString());
             var sagaId = MDC.putCloseable("sagaId", event.sagaId().toString());
             var eventId = MDC.putCloseable("eventId", event.eventId().toString());
             var eventType = MDC.putCloseable("eventType", event.eventType())) {
            boolean processed = idempotentConsumer.handle(CONSUMER_GROUP, event, projection::apply);
            if (!processed) {
                log.info("Skipped duplicate event");
            }
            return processed;
        }
    }
}
