-- One decision per order. Evaluate is idempotent: a repeated call returns this row (ADR 0005).
CREATE TABLE decisions (
    order_id       uuid        PRIMARY KEY,
    saga_id        uuid        NOT NULL,
    customer_id    text        NOT NULL,
    amount_cents   bigint      NOT NULL,
    currency       char(3)     NOT NULL,
    payment_method text        NOT NULL,
    decision       text        NOT NULL CHECK (decision IN ('APPROVE', 'REJECT')),
    reasons        text[]      NOT NULL DEFAULT '{}',
    evaluated_at   timestamptz NOT NULL DEFAULT now()
);

-- Velocity rule: recent orders per customer.
CREATE INDEX decisions_customer_time_idx ON decisions (customer_id, evaluated_at);

CREATE TABLE blocklist (
    kind       text        NOT NULL CHECK (kind IN ('CUSTOMER', 'PAYMENT_METHOD')),
    value      text        NOT NULL,
    reason     text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, value)
);

-- Seeds for local runs and scenario S5. pm_card_radarBlock is Stripe's test token for a Radar block.
INSERT INTO blocklist (kind, value, reason) VALUES
    ('CUSTOMER', 'blocked-customer', 'seeded test customer'),
    ('PAYMENT_METHOD', 'pm_card_radarBlock', 'Stripe test token for a fraud block');
