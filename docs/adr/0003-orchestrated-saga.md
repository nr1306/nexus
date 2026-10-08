# ADR 0003 — Orchestrated saga in the Order service

- **Status:** Accepted
- **Date:** 2026-10-07
- **Scope:** `services/order` (`saga` package), commands to Inventory and Payment

## Context

An order touches stock (Inventory) and money (Payment). There is no distributed transaction across their databases, yet each order must end fully `COMPLETED` or fully `CANCELLED`, and must survive crashes, duplicates and lost replies.

## Decision

1. **Orchestration, not choreography.** The Order service is the only component that knows the flow. It sends one command at a time and waits for that command's reply. Inventory and Payment only answer commands; they never react to each other's events.
2. **Persisted state machine.** `saga_instances` stores `state`, the `step` awaiting a reply, `step_deadline`, `step_attempts` and the remaining `compensations`. Every transition updates this row and writes the next command to the outbox in **one transaction**, with the row locked (`SELECT … FOR UPDATE`) so replies and timeouts for one order never interleave (rules 1 and 9). A restarted pod simply continues from the row.
3. **Phase 1 flow:** `ReserveInventory → AuthorizePayment → CapturePayment → COMPLETED`. On completion it sends `CommitInventory` (fire-and-forget; settles the stock hold) and publishes `OrderCompleted`. Phase 2 inserts the fraud check and fulfillment steps.
4. **Replies are matched strictly.** A reply is applied only if its `sagaId` matches and its type is the success or failure reply of the step being awaited. Anything else is logged and ignored: duplicates (also deduped by `processed_events`), replies to re-sent commands, late replies after cancellation, and replies for other sagas.
5. **Compensation runs in reverse, one step at a time.** The plan is computed when the saga fails:
   - business failure → undo the forward steps that completed, most recent first;
   - **timeout** → also undo the in-flight step, because it may have succeeded without us hearing back. Compensations are idempotent, so undoing something that never happened is a no-op.

   | Failure | Plan |
   |---|---|
   | `InventoryRejected` | none → `CANCELLED` |
   | `PaymentDeclined` | `ReleaseInventory` |
   | `CaptureFailed` | `VoidPayment`, `ReleaseInventory` |
   | timeout on reserve / authorize / capture | as above plus the in-flight step's undo |

   Capture has no undo in Phase 1 (refund arrives in Phase 2). After a capture timeout the void runs and, if the capture had in fact happened, fails with `ALREADY_CAPTURED` → `NEEDS_ATTENTION`.
   *Superseded by ADR 0006:* capture is undone by `RefundPayment`, so a capture timeout now ends `CANCELLED` with the money refunded.
6. **Deadlines.** Each step waits `nexus.order.step-timeout` (30 s). A scheduled sweeper finds overdue sagas and claims each with `FOR UPDATE SKIP LOCKED` (safe on several pods). A forward step is re-sent once (`max-step-attempts: 2`) and then compensated. A compensation step is re-sent up to `max-compensation-attempts` (5) and then the saga goes to `NEEDS_ATTENTION`. Re-sent commands get a new `eventId`; receivers stay correct because they are idempotent per order (ADR 0002, Inventory's existing-hold check).
7. **Per-order ordering is relied on.** All commands for an order go to the same partition of `inventory.commands` / `payment.commands` (key = `orderId`), so e.g. `ReserveInventory` is always processed before a later `ReleaseInventory` or `CommitInventory`.

## Alternatives considered

- **Choreography** (each service reacts to the previous service's event). Fewer moving parts per service, but the flow and its compensations are spread across services, there is no single place to see or debug a saga's state, and timeouts have no natural owner. Rejected; SPEC lists "one place to see and debug saga state" as the reason.
- **Parallel compensations.** Faster cancellation, but harder to reason about and to observe. Sequential reverse order is the textbook guarantee and the latency is irrelevant for failures. Rejected for now.
- **Saga framework** (Axon, Eventuate, Temporal). Hides exactly the mechanics this project is meant to demonstrate, and adds infrastructure. Rejected.
- **Inventory reacting to `OrderCompleted` to settle holds.** Considered first. The command keeps the flow in one orchestrator and guarantees per-partition ordering with reserve/release. Chosen: `CommitInventory`.

## Consequences

- Saga metrics: `saga_completed_total`, `saga_duration_seconds`, `saga_compensating_total{reason}` (entered COMPENSATING), `saga_compensated_total{reason}` (COMPENSATING → CANCELLED), `saga_cancelled_total{reason}`, `saga_needs_attention_total{reason}`. *Compensation success* = compensated / compensating. All are recorded after commit.
- The sweeper polls every second (`nexus.order.timeout-sweeper.interval`) with an indexed query. The cost is negligible, but it is a constant load per pod.
- `NEEDS_ATTENTION` sagas are terminal and need Reconciliation (Phase 3) or an operator.
- Fraud (Phase 2) slots between authorize and capture; its failure plan is `VoidPayment, ReleaseInventory`, which already matches the capture-failure plan.
