package com.nexus.messaging.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.nexus.messaging.envelope.EventEnvelope;

import java.time.Instant;
import java.util.UUID;

public final class TestEnvelopes {

    public static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .findAndAddModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private TestEnvelopes() {
    }

    public static EventEnvelope reserveInventory(UUID orderId) {
        var payload = OBJECT_MAPPER.createObjectNode();
        payload.putArray("items").addObject().put("sku", "SKU-1").put("quantity", 2);
        return new EventEnvelope(UUID.randomUUID(), "ReserveInventory", 1, orderId, UUID.randomUUID(),
                Instant.parse("2026-10-07T12:00:00.123456Z"), payload);
    }
}
