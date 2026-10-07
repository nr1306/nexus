package com.nexus.order.messaging;

import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.idempotency.IdempotentConsumer;
import com.nexus.order.saga.SagaOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * Applies one reply event to its saga at most once (CLAUDE.md rule 2).
 */
@Component
public class SagaReplyHandler {

    public static final String CONSUMER_GROUP = "order-saga";

    private static final Logger log = LoggerFactory.getLogger(SagaReplyHandler.class);

    private final IdempotentConsumer idempotentConsumer;
    private final SagaOrchestrator orchestrator;

    public SagaReplyHandler(IdempotentConsumer idempotentConsumer, SagaOrchestrator orchestrator) {
        this.idempotentConsumer = idempotentConsumer;
        this.orchestrator = orchestrator;
    }

    /** @return {@code false} if the reply was a duplicate and was skipped */
    public boolean handle(EventEnvelope reply) {
        try (var orderId = MDC.putCloseable("orderId", reply.orderId().toString());
             var sagaId = MDC.putCloseable("sagaId", reply.sagaId().toString());
             var eventId = MDC.putCloseable("eventId", reply.eventId().toString());
             var eventType = MDC.putCloseable("eventType", reply.eventType())) {
            boolean processed = idempotentConsumer.handle(CONSUMER_GROUP, reply, orchestrator::onReply);
            if (!processed) {
                log.info("Skipped duplicate reply");
            }
            return processed;
        }
    }
}
