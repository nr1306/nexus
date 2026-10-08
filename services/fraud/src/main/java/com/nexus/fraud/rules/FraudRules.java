package com.nexus.fraud.rules;

import java.util.ArrayList;
import java.util.List;

/**
 * The rules engine: pure, so every rule is unit-testable. An order is rejected if any rule fires.
 */
public final class FraudRules {

    public static final String AMOUNT_LIMIT = "AMOUNT_LIMIT";
    public static final String VELOCITY = "VELOCITY";
    public static final String BLOCKLISTED_CUSTOMER = "BLOCKLISTED_CUSTOMER";
    public static final String BLOCKLISTED_PAYMENT_METHOD = "BLOCKLISTED_PAYMENT_METHOD";

    /** What the rules look at, gathered by the caller. */
    public record Facts(long amountCents, int recentOrdersByCustomer, boolean customerBlocked, boolean paymentMethodBlocked) {
    }

    public record Limits(long maxAmountCents, int velocityMaxOrders) {
    }

    private FraudRules() {
    }

    /** @return the rules that fired; empty means APPROVE */
    public static List<String> evaluate(Facts facts, Limits limits) {
        List<String> reasons = new ArrayList<>();
        if (facts.amountCents() > limits.maxAmountCents()) {
            reasons.add(AMOUNT_LIMIT);
        }
        if (facts.recentOrdersByCustomer() >= limits.velocityMaxOrders()) {
            reasons.add(VELOCITY);
        }
        if (facts.customerBlocked()) {
            reasons.add(BLOCKLISTED_CUSTOMER);
        }
        if (facts.paymentMethodBlocked()) {
            reasons.add(BLOCKLISTED_PAYMENT_METHOD);
        }
        return List.copyOf(reasons);
    }
}
