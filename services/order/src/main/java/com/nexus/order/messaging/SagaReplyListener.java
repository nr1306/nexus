package com.nexus.order.messaging;

import com.nexus.messaging.envelope.EnvelopeMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes saga replies from Inventory, Payment and Fulfillment. The offset is committed after {@link #onReply}
 * returns, i.e. after the saga transition has committed (CLAUDE.md rule 8).
 */
@Component
class SagaReplyListener {

    private final EnvelopeMapper envelopeMapper;
    private final SagaReplyHandler handler;

    SagaReplyListener(EnvelopeMapper envelopeMapper, SagaReplyHandler handler) {
        this.envelopeMapper = envelopeMapper;
        this.handler = handler;
    }

    @KafkaListener(topics = {OrderTopics.INVENTORY_EVENTS, OrderTopics.PAYMENT_EVENTS, OrderTopics.FULFILLMENT_EVENTS},
            groupId = SagaReplyHandler.CONSUMER_GROUP)
    void onReply(ConsumerRecord<String, String> record) {
        handler.handle(envelopeMapper.fromJson(record.value()));
    }
}
