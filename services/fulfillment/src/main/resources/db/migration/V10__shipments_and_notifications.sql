-- One row per order (SPEC.md §6). Besides real shipments it holds markers: a CancelShipment that
-- arrives before any shipment inserts a CANCELLED row with no shipment_id, so a late CreateShipment
-- (e.g. from a retry topic) is rejected instead of shipping a cancelled order.
CREATE TABLE shipments (
    order_id        uuid        PRIMARY KEY,
    shipment_id     uuid        UNIQUE,              -- null for rejected requests and cancel markers
    status          text        NOT NULL CHECK (status IN ('CREATED', 'SHIPPED', 'CANCELLED', 'REJECTED')),
    customer_id     text,
    items           jsonb,
    failure_reason  text,
    tracking_number text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- At most one notification of each type per order: a redelivered event can't notify twice.
CREATE TABLE notifications (
    id          uuid        PRIMARY KEY,
    order_id    uuid        NOT NULL,
    type        text        NOT NULL CHECK (type IN ('ORDER_CONFIRMED', 'ORDER_SHIPPED', 'ORDER_CANCELLED')),
    customer_id text,
    channel     text        NOT NULL,
    body        text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (order_id, type)
);

-- SKUs the simulated carrier refuses to ship; a CreateShipment containing one fails (saga step 5's
-- business failure). SKU-HAZMAT-01 is seeded in Inventory's catalog for local runs and e2e tests.
CREATE TABLE restricted_skus (
    sku    text PRIMARY KEY,
    reason text NOT NULL
);

INSERT INTO restricted_skus (sku, reason) VALUES ('SKU-HAZMAT-01', 'HAZMAT');
