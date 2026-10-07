package com.nexus.payment.messaging;

import com.nexus.payment.config.PaymentProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
public class PaymentTopics {

    public static final String COMMANDS = "payment.commands";
    public static final String EVENTS = "payment.events";

    @Bean
    NewTopic paymentCommandsTopic(PaymentProperties properties) {
        return TopicBuilder.name(COMMANDS)
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }

    @Bean
    NewTopic paymentEventsTopic(PaymentProperties properties) {
        return TopicBuilder.name(EVENTS)
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }
}
