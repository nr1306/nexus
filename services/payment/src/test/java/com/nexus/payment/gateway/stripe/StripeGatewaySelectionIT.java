package com.nexus.payment.gateway.stripe;

import com.nexus.payment.PaymentIntegrationTest;
import com.nexus.payment.gateway.PaymentGateway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code PAYMENT_GATEWAY=stripe} swaps the mock for the Stripe adapter; no call is made at startup. */
@TestPropertySource(properties = {
        "nexus.payment.gateway=stripe",
        "nexus.payment.stripe.api-key=sk_test_wiring_only",
        "nexus.payment.stripe.base-url=http://localhost:9"})
class StripeGatewaySelectionIT extends PaymentIntegrationTest {

    @Autowired
    PaymentGateway gateway;

    @Test
    void stripeAdapterIsTheGateway() {
        assertThat(gateway).isInstanceOf(StripePaymentGateway.class);
        assertThat(gateway.name()).isEqualTo("stripe");
    }
}
