-- Stock per SKU. Oversell prevention relies on the conditional update in StockRepository
-- (CLAUDE.md rule 5); the CHECK constraints are a last line of defence.
CREATE TABLE stock (
    sku        text        PRIMARY KEY,
    available  integer     NOT NULL CHECK (available >= 0),
    reserved   integer     NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One row per (order, SKU). HELD rows count towards stock.reserved; RELEASED rows are kept for audit
-- and so a late ReserveInventory for a released order is rejected instead of re-reserving.
CREATE TABLE reservations (
    order_id    uuid        NOT NULL,
    sku         text        NOT NULL REFERENCES stock (sku),
    quantity    integer     NOT NULL CHECK (quantity > 0),
    status      text        NOT NULL CHECK (status IN ('HELD', 'RELEASED')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL,
    released_at timestamptz,
    PRIMARY KEY (order_id, sku)
);

-- For the expiry sweeper (SPEC.md §7).
CREATE INDEX reservations_held_expiry_idx ON reservations (expires_at) WHERE status = 'HELD';
