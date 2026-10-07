package com.nexus.inventory.messaging;

import com.nexus.messaging.envelope.EnvelopeMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code inventory.commands}. The offset is committed after {@link #onCommand} returns, i.e.
 * after the database transaction has committed (CLAUDE.md rule 8).
 */
@Component
class InventoryCommandListener {

    private final EnvelopeMapper envelopeMapper;
    private final InventoryCommandHandler handler;

    InventoryCommandListener(EnvelopeMapper envelopeMapper, InventoryCommandHandler handler) {
        this.envelopeMapper = envelopeMapper;
        this.handler = handler;
    }

    @KafkaListener(topics = InventoryTopics.COMMANDS, groupId = InventoryCommandHandler.CONSUMER_GROUP)
    void onCommand(ConsumerRecord<String, String> record) {
        handler.handle(envelopeMapper.fromJson(record.value()));
    }
}
