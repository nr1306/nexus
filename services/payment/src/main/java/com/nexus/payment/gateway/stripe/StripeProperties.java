package com.nexus.payment.gateway.stripe;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Stripe test mode (ADR 0010). The key comes from the {@code STRIPE_API_KEY} environment variable, never
 * from code or config files; only test-mode keys are accepted.
 *
 * @param apiKey                a {@code sk_test_} / {@code rk_test_} key
 * @param baseUrl               Stripe's API; tests point it at a stub server
 * @param apiVersion            pinned {@code Stripe-Version}, so responses don't change under us
 * @param connectTimeout        TCP connect timeout
 * @param requestTimeout        whole-request timeout; a timeout is a transient error (retried, same idempotency key)
 * @param failureRateThreshold  circuit breaker: % of failed calls (errors, not declines) that opens it
 * @param slidingWindowSize     circuit breaker: calls in the window
 * @param minimumCalls          circuit breaker: calls before the rate is evaluated
 * @param openStateWait         circuit breaker: how long it stays open before trying again
 */
@ConfigurationProperties("nexus.payment.stripe")
public record StripeProperties(
        String apiKey,
        @DefaultValue("https://api.stripe.com") String baseUrl,
        @DefaultValue("2024-06-20") String apiVersion,
        @DefaultValue("PT5S") Duration connectTimeout,
        @DefaultValue("PT20S") Duration requestTimeout,
        @DefaultValue("50") float failureRateThreshold,
        @DefaultValue("20") int slidingWindowSize,
        @DefaultValue("10") int minimumCalls,
        @DefaultValue("PT30S") Duration openStateWait) {
}
