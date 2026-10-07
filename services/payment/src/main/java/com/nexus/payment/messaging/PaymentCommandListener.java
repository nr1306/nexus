package com.nexus.payment.messaging;

import com.nexus.messaging.envelope.EnvelopeMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code payment.commands}. The offset is committed after {@link #onCommand} returns, i.e.
 * after the database transaction has committed (CLAUDE.md rule 8).
 */
@Component
class PaymentCommandListener {

    private final EnvelopeMapper envelopeMapper;
    private final PaymentCommandHandler handler;

    PaymentCommandListener(EnvelopeMapper envelopeMapper, PaymentCommandHandler handler) {
        this.envelopeMapper = envelopeMapper;
        this.handler = handler;
    }

    @KafkaListener(topics = PaymentTopics.COMMANDS, groupId = PaymentCommandHandler.CONSUMER_GROUP)
    void onCommand(ConsumerRecord<String, String> record) {
        handler.handle(envelopeMapper.fromJson(record.value()));
    }
}
