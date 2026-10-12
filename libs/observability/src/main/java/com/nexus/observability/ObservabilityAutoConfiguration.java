package com.nexus.observability;

import com.nexus.messaging.outbox.TraceparentSource;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Connects the outbox to the current trace: each outbox row gets the traceparent of the span that wrote
 * it, Debezium copies it to the Kafka {@code traceparent} header (ADR 0001), and the consumer's
 * listener observation continues the trace from there. Registered before libs/messaging's
 * {@code MessagingAutoConfiguration} so its no-op {@link TraceparentSource} is replaced; services without
 * libs/messaging (Fraud) get only {@link TraceContinuation}.
 *
 * <p>With tracing disabled (as in Spring Boot tests by default) the beans fall back to no-op tracing:
 * outbox rows get no traceparent and {@link TraceContinuation} just runs the work.
 */
@AutoConfiguration(
        afterName = {
                "org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration",
                "org.springframework.boot.actuate.autoconfigure.tracing.OpenTelemetryTracingAutoConfiguration",
                "org.springframework.boot.actuate.autoconfigure.tracing.NoopTracerAutoConfiguration"},
        beforeName = "com.nexus.messaging.config.MessagingAutoConfiguration")
public class ObservabilityAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    TraceContinuation traceContinuation(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        return new TraceContinuation(tracer.getIfAvailable(() -> Tracer.NOOP), propagator.getIfAvailable(() -> Propagator.NOOP));
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "com.nexus.messaging.outbox.TraceparentSource")
    static class OutboxTracing {

        @Bean
        @ConditionalOnMissingBean(TraceparentSource.class)
        TraceparentSource otelTraceparentSource(TraceContinuation traces) {
            return traces::current;
        }
    }
}
