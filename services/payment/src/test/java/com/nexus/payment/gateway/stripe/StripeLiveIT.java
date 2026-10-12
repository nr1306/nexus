package com.nexus.payment.gateway.stripe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.payment.gateway.PaymentGateway.AuthorizationResult;
import com.nexus.payment.gateway.PaymentGateway.OperationResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Against the real Stripe API in <b>test mode</b> (ADR 0010). Runs only when {@code STRIPE_API_KEY} holds a
 * test-mode key; uses Stripe's test payment methods, so no money moves.
 */
@EnabledIfEnvironmentVariable(named = "STRIPE_API_KEY", matches = "(sk|rk)_test_.+")
class StripeLiveIT {

    private final StripePaymentGateway stripe = new StripePaymentGateway(new StripeProperties(System.getenv("STRIPE_API_KEY"),
            "https://api.stripe.com", "2024-06-20", Duration.ofSeconds(5), Duration.ofSeconds(20), 50, 20, 10,
            Duration.ofSeconds(30)), new ObjectMapper(), new SimpleMeterRegistry());

    @Test
    void authorizeCaptureRefundAndRepeatAreIdempotent() {
        String order = UUID.randomUUID().toString();
        AuthorizationResult auth = stripe.authorize(order + ":AUTHORIZE", 1999, "USD", "pm_card_visa");
        assertThat(auth.approved()).isTrue();
        assertThat(auth.authorizationId()).startsWith("pi_");
        // Same key: Stripe returns the original PaymentIntent instead of creating a second one.
        assertThat(stripe.authorize(order + ":AUTHORIZE", 1999, "USD", "pm_card_visa")).isEqualTo(auth);

        assertThat(stripe.capture(order + ":CAPTURE", auth.authorizationId(), 1999, "USD")).isEqualTo(OperationResult.success());
        assertThat(stripe.refund(order + ":REFUND", auth.authorizationId(), 1999, "USD")).isEqualTo(OperationResult.success());
        assertThat(stripe.refund(order + ":REFUND", auth.authorizationId(), 1999, "USD")).isEqualTo(OperationResult.success());
    }

    @Test
    void authorizeThenVoid() {
        String order = UUID.randomUUID().toString();
        AuthorizationResult auth = stripe.authorize(order + ":AUTHORIZE", 2500, "USD", "pm_card_visa");

        assertThat(stripe.voidAuthorization(order + ":VOID", auth.authorizationId())).isEqualTo(OperationResult.success());
        assertThat(stripe.voidAuthorization(UUID.randomUUID() + ":VOID", auth.authorizationId())).isEqualTo(OperationResult.success());
    }

    @Test
    void testCardsAreDeclined() {
        assertThat(stripe.authorize(UUID.randomUUID() + ":AUTHORIZE", 1000, "USD", "pm_card_chargeDeclined"))
                .isEqualTo(AuthorizationResult.declined("CARD_DECLINED"));
        assertThat(stripe.authorize(UUID.randomUUID() + ":AUTHORIZE", 1000, "USD", "pm_card_chargeDeclinedInsufficientFunds"))
                .isEqualTo(AuthorizationResult.declined("INSUFFICIENT_FUNDS"));
    }
}
