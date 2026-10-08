-- Per-order release marker (ADR 0004). Retry topics can deliver a ReserveInventory after the saga already
-- released the order (e.g. the reserve was retrying when the saga timed out and compensated). Once an
-- order is here, any later reserve is rejected as ALREADY_RELEASED instead of creating a hold nothing
-- would ever release.
CREATE TABLE released_orders (
    order_id    uuid        PRIMARY KEY,
    released_at timestamptz NOT NULL DEFAULT now()
);
