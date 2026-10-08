# ADR 0007 — Reservation expiry sweeper

- **Status:** Accepted
- **Date:** 2026-10-08
- **Scope:** `services/inventory` (`ReservationExpirySweeper`), Order's reaction to `ReservationExpired`

## Context

SPEC §7 says reservations expire after 10 min and a sweeper releases the holds, so an abandoned saga can't lock stock forever. A saga normally finishes or compensates in well under the TTL: each step times out after 30 s and is retried once. So a hold that is still `HELD` after 10 min almost always belongs to a saga that is stuck, for example because Order was down. That saga may still resume afterwards, so expiry must stay safe if it does.

## Decision

1. **The sweeper.** `ReservationExpirySweeper` runs every 10 s (`nexus.inventory.expiry-sweeper.*`) and handles one order per transaction:
   - It turns overdue `HELD` rows into `EXPIRED` and returns their stock.
   - It inserts the `released_orders` marker, so a late `ReserveInventory` is rejected (ADR 0004).
   - It publishes `ReservationExpired` on `inventory.events` and counts `reservations_expired_total`.

   The conditional `UPDATE … WHERE status = 'HELD' AND expires_at <= now` row-locks each row. That serializes the sweeper with a concurrent release or commit and makes it safe on several pods. Migration V15 adds `EXPIRED` and `reservations.saga_id`, so the event carries the right `sagaId`; older rows use the nil UUID.
2. **Order reacts to `ReservationExpired`.** The event is not a reply to any step:
   - **Forward steps still running:** Order compensates with reason `RESERVATION_EXPIRED`, including the in-flight step, as on a timeout. Continuing would sell stock that is no longer held. The plan's `ReleaseInventory` is a no-op.
   - **Saga compensating or finished:** Order ignores the event.
3. **`CommitInventory` after expiry.** This race is possible: the saga completes just as the hold expires. The order is sold, so Inventory takes the stock again with the same conditional update as a reservation, all or nothing, and commits it.
   - **If the stock is gone:** the order is oversold. Inventory replies with an `InventoryCommitted` whose `shortfall` lists the items, logs an ERROR and counts `inventory_commit_shortfall_total`. Reconciliation (Phase 3) or an operator resolves it.

## Alternatives considered

- **Only release the stock and tell nobody.** A resumed saga would complete, and its commit would find nothing held: silent oversell. Rejected.
- **Ask Order whether the saga is finished before expiring.** That would be a cross-service read on the hot path, and Order may be the component that is down. Rejected.
- **Never expire holds.** Abandoned sagas would lock stock forever, which is SPEC §7's exact failure. Rejected.

## Consequences

- The TTL must stay well above the longest normal saga: 5 forward steps × 2 attempts × 30 s, plus compensations. 10 min leaves a wide margin; shortening it needs this check.
- Oversell is still possible in one narrow case: the commit races the expiry and the stock was resold in between. It is detected and counted, not prevented.
- **Covered by these tests:**
  - **`ReservationExpiryIT`** covers six cases:
    - an overdue hold is released and reported;
    - fresh, committed and released holds are left alone;
    - a second sweep and a late release are no-ops;
    - a late reserve is rejected;
    - a commit after expiry re-takes the stock;
    - a commit after expiry reports the shortfall.
  - **`SagaIT`** checks that a mid-saga expiry compensates and that the event is ignored once the saga is completed or compensating.
