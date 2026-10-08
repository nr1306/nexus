package com.nexus.fraud.rules;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FraudRulesTest {

    private static final FraudRules.Limits LIMITS = new FraudRules.Limits(500_000, 5);

    @Test
    void cleanOrderIsApproved() {
        assertThat(FraudRules.evaluate(new FraudRules.Facts(10_000, 0, false, false), LIMITS)).isEmpty();
    }

    @Test
    void amountAtLimitPassesAndAboveFails() {
        assertThat(FraudRules.evaluate(new FraudRules.Facts(500_000, 0, false, false), LIMITS)).isEmpty();
        assertThat(FraudRules.evaluate(new FraudRules.Facts(500_001, 0, false, false), LIMITS))
                .containsExactly(FraudRules.AMOUNT_LIMIT);
    }

    @Test
    void velocityFiresWhenCustomerAlreadyHasMaxOrdersInWindow() {
        assertThat(FraudRules.evaluate(new FraudRules.Facts(100, 4, false, false), LIMITS)).isEmpty();
        assertThat(FraudRules.evaluate(new FraudRules.Facts(100, 5, false, false), LIMITS))
                .containsExactly(FraudRules.VELOCITY);
    }

    @Test
    void everyFiringRuleIsReported() {
        assertThat(FraudRules.evaluate(new FraudRules.Facts(900_000, 9, true, true), LIMITS)).containsExactly(
                FraudRules.AMOUNT_LIMIT, FraudRules.VELOCITY,
                FraudRules.BLOCKLISTED_CUSTOMER, FraudRules.BLOCKLISTED_PAYMENT_METHOD);
    }
}
