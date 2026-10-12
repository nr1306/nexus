package com.nexus.reconciliation.finding;

import java.time.Instant;
import java.util.UUID;

public record Finding(UUID orderId, FindingType type, UUID sagaId, Instant detectedAt, Instant resolvedAt,
                      String repairCommand, int repairAttempts, Instant lastRepairAt) {
}
