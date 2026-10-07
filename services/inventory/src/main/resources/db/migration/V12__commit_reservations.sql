-- A completed order's hold is COMMITTED: the stock has left the system (stock.reserved decreases,
-- available is unchanged). Committed holds are never released or expired.
ALTER TABLE reservations DROP CONSTRAINT reservations_status_check;
ALTER TABLE reservations ADD CONSTRAINT reservations_status_check CHECK (status IN ('HELD', 'RELEASED', 'COMMITTED'));
ALTER TABLE reservations ADD COLUMN committed_at timestamptz;
