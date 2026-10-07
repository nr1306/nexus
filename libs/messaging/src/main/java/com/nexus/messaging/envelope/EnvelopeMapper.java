package com.nexus.messaging.envelope;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serializes envelopes to and from the JSON stored in the outbox and carried on Kafka.
 */
public class EnvelopeMapper {

    private final ObjectMapper objectMapper;

    public EnvelopeMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String toJson(EventEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize envelope " + envelope.eventId(), e);
        }
    }

    /**
     * @throws InvalidEnvelopeException if the JSON is not a valid envelope (a permanent error)
     */
    public EventEnvelope fromJson(String json) {
        try {
            return objectMapper.readValue(json, EventEnvelope.class);
        } catch (JsonProcessingException e) {
            throw new InvalidEnvelopeException("Invalid event envelope", e);
        }
    }
}
