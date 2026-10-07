package com.nexus.messaging.envelope;

import com.nexus.messaging.support.TestEnvelopes;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvelopeMapperTest {

    private final EnvelopeMapper mapper = new EnvelopeMapper(TestEnvelopes.OBJECT_MAPPER);

    @Test
    void roundTripsEnvelopeWithUtcIsoTimestamp() {
        EventEnvelope envelope = TestEnvelopes.reserveInventory(UUID.randomUUID());

        String json = mapper.toJson(envelope);

        assertThat(json).contains("\"occurredAt\":\"2026-10-07T12:00:00.123456Z\"");
        assertThat(mapper.fromJson(json)).isEqualTo(envelope);
    }

    @Test
    void rejectsMalformedJsonAsInvalidEnvelope() {
        assertThatThrownBy(() -> mapper.fromJson("{not json"))
                .isInstanceOf(InvalidEnvelopeException.class);
    }

    @Test
    void rejectsEnvelopeMissingRequiredFields() {
        String missingOrderId = """
                {"eventId":"%s","eventType":"ReserveInventory","schemaVersion":1,
                 "sagaId":"%s","occurredAt":"2026-10-07T12:00:00Z","payload":{}}
                """.formatted(UUID.randomUUID(), UUID.randomUUID());

        assertThatThrownBy(() -> mapper.fromJson(missingOrderId))
                .isInstanceOf(InvalidEnvelopeException.class);
    }
}
