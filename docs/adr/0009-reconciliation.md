# ADR 0009 — Reconciliation: an event-sourced order view, findings and repairs

- **Status:** Accepted
- **Date:** 2026-10-10
- **Scope:** `services/reconciliation`, the S6 scenario (`ReconciliationCheckIT`)

## Context

SPEC §7 asks for a Reconciliation service that builds a per-order view from all `*.events` topics every 60 s, checks five invariants and issues repair commands. The Phase 3 done criterion is that every S6-injected inconsistency is detected locally.

Reconciliation may only see events (CLAUDE.md rule 4). S6 "injects inconsistencies into service DBs", but a plain row edit emits no event and so can never be detected. Each S6 injection is therefore designed together with the detector that catches it: it simulates a bug that made a service **publish** something wrong, so both the service's row and its outbox row are written.

Events for one order arrive on four topics with no ordering between them, and retry topics reorder them within a topic too (ADR 0004). A view that changes state in event order would raise false alarms.

## Decision

1. **Service:** Java, port 8106, its own `reconciliation_db`, one consumer group `reconciliation` reading `order.events`, `inventory.events`, `payment.events` and `fulfillment.events`. It uses `libs/messaging`: idempotent consumption, retry topics and DLQ, and the outbox for repair commands.
2. **Projection as facts, not a state machine.** `order_projection` has one row per order and a timestamp column per fact:
   - **Order facts:** created, completed, cancelled, needs-attention.
   - **Inventory facts:** last reserved, last released, committed, expired.
   - **Payment facts:** authorized, captured, refunded, voided. `refunded=false` and `voided=false` replies don't count.
   - **Fulfillment facts:** shipment created, shipment cancelled, shipped.
   - **Plus:** `saga_id` and `last_event_at`, the time the last event for the order was *received*.

   Each fact keeps the latest `occurredAt` (`GREATEST`), so applying events in any order, or twice, gives the same row. "Stock is held" means the last reserve is newer than the last release, commit and expiry.
3. **Checks** run every `nexus.reconciliation.check-interval` (60 s). They only look at orders that have been quiet for `settle-window` (2 min): no event received in that time. That skips sagas mid-flight and lets fire-and-forget follow-ups, such as `CommitInventory`, land first. The window must exceed how late a follow-up can legitimately be: the retry chain (about 36 s, ADR 0004) plus a crashed consumer's handover (about 10 s, ADR 0008). In a local trial of the Phase 2 check, a 10 s window flagged completed orders whose `CommitInventory` arrived late because Inventory had just been killed (see Consequences).

   | # | Finding | Condition | Repair |
   |---|---|---|---|
   | 1 | `COMPLETED_NOT_CAPTURED` / `COMPLETED_NOT_SHIPPED` | completed, but no capture or no shipment created | none: flag only (see below) |
   | 2 | `CANCELLED_BUT_CHARGED` | cancelled, captured, not refunded | `RefundPayment` |
   | 3 | `STOCK_HELD_AFTER_COMPLETION` | completed, and stock held | `CommitInventory` |
   | 3 | `STOCK_HELD_AFTER_CANCELLATION` | cancelled, and stock held | `ReleaseInventory` |
   | 4 | `SAGA_STUCK` | created, not terminal, quiet for `stuck-after` (5 min), or `NEEDS_ATTENTION` | none: flag only |
   | 5 | per-SKU reserved = held | deferred, see below | — |

4. **Findings and repairs.**
   - `findings` is unique on `(order_id, finding_type)`, so a mismatch is reported once. It stores `detected_at`, then `resolved_at` once a later check finds the invariant holds again.
   - The metric `reconciliation_findings_total{type}` counts detections; the gauge `reconciliation_open_findings` counts open ones.
   - A repairable finding writes its command through the outbox in the same transaction as the finding. The command carries the order's `sagaId` and reason `RECONCILIATION_<type>`.
   - If the finding is still open after `repair-retry-after` (2 min), the command is re-sent, up to 3 times. After that the finding stays open for an operator.
   - Both repairs are idempotent in the receiving service. The replies land on topics Order consumes, and Order ignores them because the saga is terminal.
   - `GET /admin/findings?open=true` lists findings. It is internal only, like `/admin/dlq`.
5. **Why some invariants are flag-only.**
   - **#1:** the missing step can't be safely re-run afterwards. A capture needs a live authorization, and a new `CreateShipment` for a completed order is never dispatched, because Fulfillment dispatches only on `OrderCompleted`.
   - **#4:** "resume or compensate" needs a command channel into Order (`order.commands`), which doesn't exist yet.

   Both are listed as Phase 3 follow-ups.
6. **#5 is deferred.** Per-SKU `stock.reserved` never appears in an event, and Reconciliation may not read `inventory_db`. Doing it properly means Inventory checking itself and publishing a drift event. That event wouldn't be keyed by `orderId` (rule 3), so it needs its own decision. Until then the per-SKU check stays in the done checks (`stockDrift`), which read `inventory_db` from the test.
7. **S6: `ReconciliationCheckIT`, run with `make s6-check`.**
   - **Normal load first:** it runs a mixed load with no faults and asserts **0 findings**, a false-positive check.
   - **Then injections:** for N orders per type, it writes the faulty row plus its outbox row into the owning service's database, recording the injection time.
     - **#1:** in `order_db`, a completed saga plus an `OrderCompleted` row for a new order that has no payment and no shipment.
     - **#2:** in `payment_db`, a `CAPTURE:SUCCEEDED` row plus `PaymentCaptured`, for an order cancelled by a card decline.
     - **#3:** in `inventory_db`, a HELD reservation for a new SKU plus its `reserved` increment plus `InventoryReserved`, once for a cancelled order (repaired by a release) and once for a completed one (repaired by a commit).
     - **#4:** in `order_db`, an order plus a `PENDING` saga whose deadline is a day away, plus `OrderCreated`, with no command sent.
   - **Measures** detection rate, time to detect (detection time minus injection time) and auto-repair rate. Results go to `bench/results/s6-reconciliation-*.json`.
   - **Timings:** the test shortens the check interval, settle window and stuck threshold, and records the values it used.

## Alternatives considered

- **Query each service's database or API.** Simpler checks, but it breaks rule 4 and couples Reconciliation to every schema. Rejected.
- **Order the projection by `occurredAt`, as a state machine.** Clocks and retries make this fragile, and facts with `GREATEST` are order-free. Rejected.
- **Repair #1 by re-issuing commands, as SPEC suggests.** Unsafe, for the reasons in 5. Rejected for now.

## Consequences

- Reconciliation sees inconsistencies only when a service *published* something inconsistent, or failed to publish something it should have. Silent corruption in a database, with no event, is out of reach by design. That's the price of rule 4.
- Detection takes up to the settle window plus one check interval: about 3 min with production settings.
- **Stock held after the end is repaired by outcome.** A completed order's hold is committed, a cancelled one's released. The first version released both. In the trial above the late `CommitInventory` happened to land before the release, so nothing was lost, but a release that overtook a commit in a retry topic would have put sold units back on sale.
- A finding can still be a delayed follow-up rather than a bug, if a consumer is down for longer than the settle window. The repair commands are idempotent and match the order's outcome, so acting on such a finding is harmless. The Phase 2 done check asserts 0 findings while each service is killed in turn.
- Reconciliation consumes `order.events` in its own group. ADR 0004's per-group retry topic names keep its retries separate from Fulfillment's.
