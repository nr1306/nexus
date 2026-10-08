package com.nexus.fulfillment.messaging;

import com.nexus.fulfillment.config.FulfillmentProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
public class FulfillmentTopics {

    public static final String COMMANDS = "fulfillment.commands";
    public static final String EVENTS = "fulfillment.events";
    /** Order's events drive dispatch and notifications. Owned by Order; declared here too so this service can start first. */
    public static final String ORDER_EVENTS = "order.events";

    @Bean
    NewTopic fulfillmentCommandsTopic(FulfillmentProperties properties) {
        return TopicBuilder.name(COMMANDS)
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }

    @Bean
    NewTopic fulfillmentEventsTopic(FulfillmentProperties properties) {
        return TopicBuilder.name(EVENTS)
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }

    @Bean
    NewTopic orderEventsTopic(FulfillmentProperties properties) {
        return TopicBuilder.name(ORDER_EVENTS)
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }
}
