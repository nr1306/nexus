-- Per-order view built from every *.events topic (ADR 0009). One timestamp per fact, each the latest
-- occurredAt seen (GREATEST), so applying events out of order or twice gives the same row.
CREATE TABLE order_projection (
    order_id              uuid        PRIMARY KEY,
    saga_id               uuid,
    created_at            timestamptz,
    completed_at          timestamptz,
    cancelled_at          timestamptz,
    needs_attention_at    timestamptz,
    reserved_at           timestamptz,
    released_at           timestamptz,
    committed_at          timestamptz,
    expired_at            timestamptz,
    authorized_at         timestamptz,
    captured_at           timestamptz,
    refunded_at           timestamptz,
    voided_at             timestamptz,
    shipment_created_at   timestamptz,
    shipment_cancelled_at timestamptz,
    shipped_at            timestamptz,
    last_event_at         timestamptz NOT NULL  -- when the last event for the order was received
);

CREATE INDEX order_projection_last_event_idx ON order_projection (last_event_at);

-- A mismatch is reported once per (order, type) (SPEC §6). resolved_at is set when a later check
-- finds the invariant holds again.
CREATE TABLE findings (
    order_id        uuid        NOT NULL,
    finding_type    text        NOT NULL,
    saga_id         uuid,
    detected_at     timestamptz NOT NULL,
    resolved_at     timestamptz,
    repair_command  text,                  -- event type of the repair command, null when flag-only
    repair_attempts integer     NOT NULL DEFAULT 0,
    last_repair_at  timestamptz,
    PRIMARY KEY (order_id, finding_type)
);

CREATE INDEX findings_open_idx ON findings (finding_type) WHERE resolved_at IS NULL;
