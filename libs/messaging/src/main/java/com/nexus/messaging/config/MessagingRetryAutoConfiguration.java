package com.nexus.messaging.config;

import com.nexus.messaging.dlq.DeadLetterRecorder;
import com.nexus.messaging.dlq.DlqAdmin;
import com.nexus.messaging.dlq.DlqAdminController;
import com.nexus.messaging.envelope.InvalidEnvelopeException;
import com.nexus.messaging.retry.FixedDelaysBackOffPolicy;
import com.nexus.messaging.retry.RetryProperties;
import com.nexus.messaging.retry.RetryTopicNaming;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicComponentFactory;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.List;

/**
 * Retry topics, DLQ and DLQ admin for services with a Kafka consumer group (ADR 0004).
 * Transient failures go through one retry topic per configured delay, then to the DLQ; permanent
 * failures (unparseable envelope, deserialization) go straight to the DLQ.
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnProperty(name = {"spring.kafka.consumer.group-id"})
@ConditionalOnProperty(name = "nexus.messaging.retry.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RetryProperties.class)
public class MessagingRetryAutoConfiguration {

    static final String DLQ_RECORDER = "nexusDeadLetterRecorder";

    /** Spring Kafka picks up this factory to name retry topics (see {@link RetryTopicNaming}). */
    @Bean
    RetryTopicComponentFactory retryTopicComponentFactory() {
        return new RetryTopicComponentFactory() {
            @Override
            public RetryTopicNamesProviderFactory retryTopicNamesProviderFactory() {
                return RetryTopicNaming.namesProviderFactory();
            }
        };
    }

    @Bean(destroyMethod = "shutdown")
    ThreadPoolTaskScheduler nexusRetryTopicScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("retry-topic-");
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    RetryTopicSchedulerWrapper retryTopicSchedulerWrapper(ThreadPoolTaskScheduler nexusRetryTopicScheduler) {
        return new RetryTopicSchedulerWrapper(nexusRetryTopicScheduler);
    }

    @Bean(DLQ_RECORDER)
    DeadLetterRecorder nexusDeadLetterRecorder(ObjectProvider<MeterRegistry> meterRegistry) {
        return new DeadLetterRecorder(meterRegistry.getIfAvailable(SimpleMeterRegistry::new));
    }

    @Bean
    RetryTopicConfiguration nexusRetryTopicConfiguration(KafkaTemplate<?, ?> kafkaTemplate, RetryProperties properties,
                                                         @Value("${spring.kafka.consumer.group-id}") String group) {
        return RetryTopicConfigurationBuilder.newInstance()
                .customBackoff(new FixedDelaysBackOffPolicy(properties.delays()))
                .maxAttempts(properties.delays().size() + 1)
                .retryTopicSuffix(RetryTopicNaming.retrySuffix(group))
                .dltSuffix(RetryTopicNaming.dlqSuffix(group))
                .notRetryOn(List.of(InvalidEnvelopeException.class))
                .traversingCauses()
                .autoCreateTopicsWith(properties.partitions(), properties.replicas())
                .dltHandlerMethod(DLQ_RECORDER, "onDeadLetter")
                .create(kafkaTemplate);
    }

    @Bean
    @SuppressWarnings("unchecked")
    DlqAdmin dlqAdmin(@Value("${spring.kafka.consumer.group-id}") String group, ConsumerFactory<?, ?> consumerFactory,
                      KafkaTemplate<?, ?> kafkaTemplate, KafkaAdmin kafkaAdmin) {
        return new DlqAdmin(group, (ConsumerFactory<String, String>) consumerFactory,
                (KafkaTemplate<String, String>) kafkaTemplate, kafkaAdmin);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    static class DlqAdminWebConfiguration {

        @Bean
        DlqAdminController dlqAdminController(DlqAdmin dlqAdmin) {
            return new DlqAdminController(dlqAdmin);
        }
    }
}
