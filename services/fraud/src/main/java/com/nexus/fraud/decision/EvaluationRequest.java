package com.nexus.fraud.decision;

import java.util.UUID;

public record EvaluationRequest(
        UUID orderId,
        UUID sagaId,
        String customerId,
        long amountCents,
        String currency,
        String paymentMethod) {
}
