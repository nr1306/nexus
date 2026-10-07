CREATE TABLE orders (
    id             uuid        PRIMARY KEY,
    customer_id    text        NOT NULL,
    total_cents    bigint      NOT NULL CHECK (total_cents > 0),
    currency       char(3)     NOT NULL,
    payment_method text        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id         uuid    NOT NULL REFERENCES orders (id),
    sku              text    NOT NULL,
    quantity         integer NOT NULL CHECK (quantity > 0),
    unit_price_cents bigint  NOT NULL CHECK (unit_price_cents > 0),
    PRIMARY KEY (order_id, sku)
);

-- API Idempotency-Key: a retried POST returns the original order. request_hash detects the same key
-- being reused with a different body. The key is claimed before the order row is inserted (same
-- transaction), so there is deliberately no foreign key to orders.
CREATE TABLE idempotency_keys (
    key          text        PRIMARY KEY,
    request_hash text        NOT NULL,
    order_id     uuid        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Saga state (ADR 0003). Persisted in the same transaction as the command it emits (CLAUDE.md rule 9).
CREATE TABLE saga_instances (
    saga_id        uuid        PRIMARY KEY,
    order_id       uuid        NOT NULL UNIQUE REFERENCES orders (id),
    state          text        NOT NULL CHECK (state IN ('PENDING', 'INVENTORY_RESERVED', 'PAYMENT_AUTHORIZED',
                                   'COMPLETED', 'COMPENSATING', 'CANCELLED', 'NEEDS_ATTENTION')),
    step           text,                          -- step awaiting a reply; null when terminal
    step_deadline  timestamptz,                   -- when the awaited reply is overdue
    step_attempts  integer     NOT NULL DEFAULT 0,
    compensations  text[]      NOT NULL DEFAULT '{}', -- remaining compensation steps after `step`, in order
    failure_reason text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX saga_instances_deadline_idx ON saga_instances (step_deadline) WHERE step IS NOT NULL;
