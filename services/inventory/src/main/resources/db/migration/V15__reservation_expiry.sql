-- Reservation expiry (SPEC.md §7). The sweeper turns overdue HELD rows into EXPIRED (released_at = when)
-- and returns their stock. saga_id lets the ReservationExpired event reach the right saga; rows created
-- before this migration have none.
ALTER TABLE reservations ADD COLUMN saga_id uuid;
ALTER TABLE reservations DROP CONSTRAINT reservations_status_check;
ALTER TABLE reservations ADD CONSTRAINT reservations_status_check
    CHECK (status IN ('HELD', 'RELEASED', 'COMMITTED', 'EXPIRED'));
