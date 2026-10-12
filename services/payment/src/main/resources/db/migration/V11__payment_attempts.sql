-- One row per provider call, including declines and errors (SPEC.md §6, ADR 0010). Written in its own
-- transaction, so a call whose business transaction rolls back (provider error, crash, lost race) is
-- still on record. `payments` keeps the single outcome per (order, operation); this is the audit trail.
CREATE TABLE payment_attempts (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id        uuid        NOT NULL,
    operation       text        NOT NULL CHECK (operation IN ('AUTHORIZE', 'CAPTURE', 'VOID', 'REFUND')),
    provider        text        NOT NULL,
    idempotency_key text        NOT NULL,
    outcome         text        NOT NULL CHECK (outcome IN ('SUCCEEDED', 'DECLINED', 'FAILED', 'ERROR')),
    failure_reason  text,                      -- decline/failure code, or the error for ERROR
    duration_ms     integer     NOT NULL,
    attempted_at    timestamptz NOT NULL
);

CREATE INDEX payment_attempts_order_idx ON payment_attempts (order_id, attempted_at);
