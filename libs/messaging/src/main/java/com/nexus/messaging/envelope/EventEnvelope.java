package com.nexus.messaging.envelope;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Envelope carried by every Kafka message. Schema: {@code contracts/events/envelope.schema.json}.
 */
public record EventEnvelope(
        UUID eventId,
        String eventType,
        int schemaVersion,
        UUID orderId,
        UUID sagaId,
        Instant occurredAt,
        JsonNode payload) {

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(sagaId, "sagaId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(payload, "payload");
        if (eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
        if (schemaVersion < 1) {
            throw new IllegalArgumentException("schemaVersion must be >= 1");
        }
    }

    public static EventEnvelope create(String eventType, int schemaVersion, UUID orderId, UUID sagaId,
                                       JsonNode payload, Clock clock) {
        return new EventEnvelope(UUID.randomUUID(), eventType, schemaVersion, orderId, sagaId,
                clock.instant(), payload);
    }
}
