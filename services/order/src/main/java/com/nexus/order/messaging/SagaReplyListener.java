package com.nexus.order.messaging;

import com.nexus.messaging.envelope.EnvelopeMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Consumes saga replies from Inventory, Payment and Fulfillment. The offset is committed after {@link #onReply}
 * returns, i.e. after the saga transition has committed (CLAUDE.md rule 8). Partition assignment
 * opens the {@link ReplyConsumptionGate} for saga timeouts.
 */
@Component
class SagaReplyListener implements ConsumerSeekAware {

    private static final Set<String> REPLY_TOPICS =
            Set.of(OrderTopics.INVENTORY_EVENTS, OrderTopics.PAYMENT_EVENTS, OrderTopics.FULFILLMENT_EVENTS);

    private final EnvelopeMapper envelopeMapper;
    private final SagaReplyHandler handler;
    private final ReplyConsumptionGate gate;

    SagaReplyListener(EnvelopeMapper envelopeMapper, SagaReplyHandler handler, ReplyConsumptionGate gate) {
        this.envelopeMapper = envelopeMapper;
        this.handler = handler;
        this.gate = gate;
    }

    /** Also called for the retry-topic containers; only the main reply topics count. */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (assignments.keySet().stream().anyMatch(tp -> REPLY_TOPICS.contains(tp.topic()))) {
            gate.onRepliesAssigned();
        }
    }

    @KafkaListener(topics = {OrderTopics.INVENTORY_EVENTS, OrderTopics.PAYMENT_EVENTS, OrderTopics.FULFILLMENT_EVENTS},
            groupId = SagaReplyHandler.CONSUMER_GROUP)
    void onReply(ConsumerRecord<String, String> record) {
        handler.handle(envelopeMapper.fromJson(record.value()));
    }
}
