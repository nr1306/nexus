package com.nexus.fulfillment.messaging;

import com.nexus.messaging.envelope.EnvelopeMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code fulfillment.commands} (saga step 5) and {@code order.events} (dispatch and
 * notifications) in one consumer group (ADR 0006). The offset is committed after {@link #onMessage}
 * returns, i.e. after the database transaction has committed (CLAUDE.md rule 8).
 */
@Component
class FulfillmentListener {

    private final EnvelopeMapper envelopeMapper;
    private final FulfillmentHandler handler;

    FulfillmentListener(EnvelopeMapper envelopeMapper, FulfillmentHandler handler) {
        this.envelopeMapper = envelopeMapper;
        this.handler = handler;
    }

    @KafkaListener(topics = {FulfillmentTopics.COMMANDS, FulfillmentTopics.ORDER_EVENTS},
            groupId = FulfillmentHandler.CONSUMER_GROUP)
    void onMessage(ConsumerRecord<String, String> record) {
        handler.handle(envelopeMapper.fromJson(record.value()));
    }
}
