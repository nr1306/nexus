package com.nexus.order.fraud;

import java.util.UUID;

/** Published by the orchestrator when a saga starts awaiting its fraud check. */
public record FraudCheckRequested(UUID orderId) {
}
