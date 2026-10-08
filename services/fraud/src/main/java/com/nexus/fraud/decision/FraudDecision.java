package com.nexus.fraud.decision;

import java.util.List;
import java.util.UUID;

public record FraudDecision(UUID orderId, boolean approved, List<String> reasons) {

    public FraudDecision {
        reasons = List.copyOf(reasons);
    }
}
