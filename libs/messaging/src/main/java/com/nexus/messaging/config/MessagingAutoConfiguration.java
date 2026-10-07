package com.nexus.messaging.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.messaging.envelope.EnvelopeMapper;
import com.nexus.messaging.idempotency.IdempotentConsumer;
import com.nexus.messaging.outbox.OutboxWriter;
import com.nexus.messaging.outbox.TraceparentSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

@AutoConfiguration(after = {
        JacksonAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class,
        TransactionAutoConfiguration.class})
public class MessagingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    EnvelopeMapper envelopeMapper(ObjectMapper objectMapper) {
        return new EnvelopeMapper(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    TraceparentSource traceparentSource() {
        return TraceparentSource.none();
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxWriter outboxWriter(JdbcTemplate jdbcTemplate, EnvelopeMapper envelopeMapper,
                              TraceparentSource traceparentSource) {
        return new OutboxWriter(jdbcTemplate, envelopeMapper, traceparentSource);
    }

    @Bean
    @ConditionalOnMissingBean
    IdempotentConsumer idempotentConsumer(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate,
                                          ObjectProvider<MeterRegistry> meterRegistry) {
        return new IdempotentConsumer(jdbcTemplate, transactionTemplate,
                meterRegistry.getIfAvailable(SimpleMeterRegistry::new));
    }
}
