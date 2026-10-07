package com.nexus.inventory.messaging;

import com.nexus.inventory.config.InventoryProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
public class InventoryTopics {

    public static final String COMMANDS = "inventory.commands";
    public static final String EVENTS = "inventory.events";

    @Bean
    NewTopic inventoryCommandsTopic(InventoryProperties properties) {
        return TopicBuilder.name(COMMANDS)
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }

    @Bean
    NewTopic inventoryEventsTopic(InventoryProperties properties) {
        return TopicBuilder.name(EVENTS)
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }
}
