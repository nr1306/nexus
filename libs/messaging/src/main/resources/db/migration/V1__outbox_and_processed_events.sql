-- Shared messaging tables, shipped by libs/messaging and applied by every service's Flyway run.
-- Versions V1-V9 are reserved for libs/messaging; service migrations start at V10.

-- Transactional outbox (ADR 0001). Rows are written in the same transaction as the state change;
-- Debezium reads them from the WAL and publishes `payload` to `topic`, keyed by `aggregate_id`.
CREATE TABLE outbox (
    id             uuid        PRIMARY KEY,           -- = envelope eventId
    aggregate_type text        NOT NULL,              -- e.g. 'order', 'reservation', 'payment'
    aggregate_id   text        NOT NULL,              -- = orderId; Kafka message key
    event_type     text        NOT NULL,              -- copied to the eventType header
    topic          text        NOT NULL,              -- destination topic, e.g. 'inventory.commands'
    payload        jsonb       NOT NULL,              -- full event envelope
    traceparent    text,                              -- W3C trace context; copied to the traceparent header
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX outbox_created_at_idx ON outbox (created_at);

-- Idempotent consumers: one row per (consumer group, event), inserted in the same transaction
-- as the side effect. A conflict means the event was already processed.
CREATE TABLE processed_events (
    consumer_group text        NOT NULL,
    event_id       uuid        NOT NULL,
    processed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);
