# ADR 0002 — Payment provider calls: inside the consumer transaction, guarded by idempotency keys

- **Status:** Accepted
- **Date:** 2026-10-07
- **Scope:** `services/payment`

## Context

Payment must authorize, capture and void **at most once per order** (CLAUDE.md rule 6), but the side effect lives at an external provider (Stripe test mode, or the mock adapter). A provider call can't join a Postgres transaction, so there is always a window where the provider has acted but our commit hasn't happened (crash, DB error, losing a race). Kafka then redelivers the command and we call the provider again.

## Decision

1. **One immutable row per `(order_id, operation)`** in `payments` (primary key). Every outcome is stored, including declines, skips and failures, and the reply event is always built from the stored row. A retried command (new `eventId`, same order) finds the row and repeats the same answer without calling the provider.
2. **The provider call runs inside the idempotent-consumer transaction**, together with the `processed_events` insert, the `payments` insert and the outbox reply.
3. **Every provider call carries an idempotency key** `<orderId>:<operation>`, which is Stripe's `Idempotency-Key` mechanism. If our transaction rolls back after the provider acted, the redelivery sends the same key and the provider returns its original result instead of acting again. In tests, concurrent capture commands sometimes reach the provider twice; the key makes that a single charge and the primary key leaves a single row.
4. **Order of checks** (so compensations and late commands are safe):
   - Authorize after any void row → `PaymentDeclined(ORDER_CANCELLED)` without calling the provider.
   - Capture needs a successful authorization and no successful void (`NOT_AUTHORIZED`, `AUTHORIZATION_VOIDED`).
   - Void after a successful capture → `PaymentVoidFailed(ALREADY_CAPTURED)`; the saga must refund.
   - Void with no successful authorization → stored as `SKIPPED`, reply `PaymentVoided(voided=false)`.
5. **The mock adapter is stateless and deterministic.** Outcomes come from the payment method (Stripe test token names where they exist) and ids are derived from the idempotency key, so it behaves like a real provider across pod restarts (needed for S4 chaos runs).

## Alternatives considered

- **Two-phase local state** (commit a `PENDING` row, call the provider outside any transaction, then commit the result). It keeps DB connections free during the call and gives a durable record of in-flight attempts, but adds a recovery job for stuck `PENDING` rows and more states. Rejected for Phase 1. Revisit if connection-pool pressure shows up in S1 with Stripe's real latency.
- **Rely only on `processed_events`.** That doesn't cover retried commands with a new `eventId`, or the crash-after-provider window. Rejected.
- **Provider call after commit** (commit the intent, call the provider from an outbox-driven step). Correct, but it splits each operation into two messages and doubles saga latency. Rejected.

## Consequences

- A DB connection is held for the duration of each provider call. With the mock this is negligible. With Stripe it bounds throughput per pod by pool size ÷ call latency, which S1 will measure.
- Correctness depends on the provider honouring idempotency keys. Stripe does, for 24 h, which is far longer than our retry horizon. Any future provider adapter must too.
- If a void fails at the provider, it is treated as transient: the transaction rolls back and the command is redelivered. Until Phase 2 retry topics and DLQ exist, Spring Kafka's default error handler retries a few times and then skips the record. That is a known gap.
- `payment_attempts` (SPEC §6) is not built. Each operation's outcome is in `payments`, and attempt-level history can be added with a new migration when the Stripe adapter lands.
