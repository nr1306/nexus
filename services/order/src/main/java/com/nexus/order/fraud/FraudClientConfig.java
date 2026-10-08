package com.nexus.order.fraud;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.TimeUnit;

@Configuration(proxyBeanMethods = false)
class FraudClientConfig {

    @Bean(destroyMethod = "")
    ManagedChannel fraudChannel(FraudClientProperties properties) {
        return NettyChannelBuilder.forTarget(properties.target()).usePlaintext().build();
    }

    @Bean
    ChannelShutdown fraudChannelShutdown(ManagedChannel fraudChannel) {
        return new ChannelShutdown(fraudChannel);
    }

    @Bean
    CircuitBreaker fraudCircuitBreaker(FraudClientProperties properties, MeterRegistry meterRegistry) {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .failureRateThreshold(properties.failureRateThreshold())
                .slidingWindowSize(properties.slidingWindowSize())
                .minimumNumberOfCalls(properties.minimumCalls())
                .waitDurationInOpenState(properties.openStateWait())
                .build());
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry);
        return registry.circuitBreaker("fraud");
    }

    @Bean
    FraudClient fraudClient(ManagedChannel fraudChannel, CircuitBreaker fraudCircuitBreaker, FraudClientProperties properties) {
        return new GrpcFraudClient(fraudChannel, fraudCircuitBreaker, properties.callDeadline());
    }

    /** Runs fraud checks off the request/consumer threads. */
    @Bean
    ThreadPoolTaskExecutor fraudCheckExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(10_000);
        executor.setThreadNamePrefix("fraud-check-");
        return executor;
    }

    /** Closes the gRPC channel on shutdown, waiting briefly for in-flight calls. */
    static final class ChannelShutdown implements AutoCloseable {

        private final ManagedChannel channel;

        ChannelShutdown(ManagedChannel channel) {
            this.channel = channel;
        }

        @Override
        public void close() throws InterruptedException {
            channel.shutdown();
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        }
    }
}
