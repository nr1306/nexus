package com.nexus.reconciliation.messaging;

import com.nexus.messaging.envelope.EnvelopeMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes every service's events in one consumer group. The offset is committed after
 * {@link #onEvent} returns, i.e. after the projection update has committed (CLAUDE.md rule 8).
 */
@Component
class ReconciliationListener {

    private final EnvelopeMapper envelopeMapper;
    private final ReconciliationHandler handler;

    ReconciliationListener(EnvelopeMapper envelopeMapper, ReconciliationHandler handler) {
        this.envelopeMapper = envelopeMapper;
        this.handler = handler;
    }

    @KafkaListener(topics = {ReconciliationTopics.ORDER_EVENTS, ReconciliationTopics.INVENTORY_EVENTS,
            ReconciliationTopics.PAYMENT_EVENTS, ReconciliationTopics.FULFILLMENT_EVENTS},
            groupId = ReconciliationHandler.CONSUMER_GROUP)
    void onEvent(ConsumerRecord<String, String> record) {
        handler.handle(envelopeMapper.fromJson(record.value()));
    }
}
