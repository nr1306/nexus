-- One immutable row per (order, operation): authorize, capture, void and refund can each happen at
-- most once per order (CLAUDE.md rule 6). A retried command finds the row and repeats its outcome.
CREATE TABLE payments (
    order_id       uuid        NOT NULL,
    operation      text        NOT NULL CHECK (operation IN ('AUTHORIZE', 'CAPTURE', 'VOID', 'REFUND')),
    status         text        NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED', 'SKIPPED')),
    amount_cents   bigint      CHECK (amount_cents > 0),
    currency       char(3),
    provider       text        NOT NULL,
    provider_ref   text,                         -- e.g. authorization id
    failure_reason text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, operation)
);
