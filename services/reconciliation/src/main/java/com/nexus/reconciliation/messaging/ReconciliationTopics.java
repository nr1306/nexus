package com.nexus.reconciliation.messaging;

import com.nexus.reconciliation.config.ReconciliationProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics this service reads (all owned by other services; declared here too so Reconciliation can
 * start first) and the command topics its repairs go to.
 */
@Configuration(proxyBeanMethods = false)
public class ReconciliationTopics {

    public static final String ORDER_EVENTS = "order.events";
    public static final String INVENTORY_EVENTS = "inventory.events";
    public static final String PAYMENT_EVENTS = "payment.events";
    public static final String FULFILLMENT_EVENTS = "fulfillment.events";
    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String PAYMENT_COMMANDS = "payment.commands";

    @Bean
    NewTopic orderEventsTopic(ReconciliationProperties properties) {
        return topic(ORDER_EVENTS, properties);
    }

    @Bean
    NewTopic inventoryEventsTopic(ReconciliationProperties properties) {
        return topic(INVENTORY_EVENTS, properties);
    }

    @Bean
    NewTopic paymentEventsTopic(ReconciliationProperties properties) {
        return topic(PAYMENT_EVENTS, properties);
    }

    @Bean
    NewTopic fulfillmentEventsTopic(ReconciliationProperties properties) {
        return topic(FULFILLMENT_EVENTS, properties);
    }

    private static NewTopic topic(String name, ReconciliationProperties properties) {
        return TopicBuilder.name(name).partitions(properties.topicPartitions()).replicas(properties.topicReplicas()).build();
    }
}
