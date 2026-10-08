package com.nexus.order.fraud;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Order → Fraud gRPC client (ADR 0005).
 *
 * @param target              gRPC target, e.g. {@code localhost:9090} or {@code fraud:9090}
 * @param callDeadline        per-call deadline
 * @param autoRun             run checks automatically (after commit + poller); tests drive them explicitly
 * @param pollInterval        how often pending checks are retried
 * @param failureRateThreshold breaker opens when this % of recent calls failed
 * @param slidingWindowSize   calls considered for the failure rate
 * @param minimumCalls        calls needed before the failure rate is evaluated
 * @param openStateWait       how long the breaker stays open before letting trial calls through
 */
@ConfigurationProperties("nexus.order.fraud")
public record FraudClientProperties(
        @DefaultValue("localhost:9090") String target,
        @DefaultValue("PT2S") Duration callDeadline,
        @DefaultValue("true") boolean autoRun,
        @DefaultValue("PT1S") Duration pollInterval,
        @DefaultValue("50") float failureRateThreshold,
        @DefaultValue("20") int slidingWindowSize,
        @DefaultValue("10") int minimumCalls,
        @DefaultValue("PT10S") Duration openStateWait) {
}
