# ADR 0010 — Stripe test-mode adapter and payment_attempts

- **Status:** Accepted
- **Date:** 2026-10-10
- **Scope:** `services/payment` (`gateway/stripe`, `payment/ProviderCalls`, `V11__payment_attempts.sql`)

## Context

SPEC §2 and §12 (Phase 2) call for a Stripe test-mode adapter next to the mock, wrapped in a Resilience4j circuit breaker (SPEC §8). SPEC §6 lists a `payment_attempts` table, which ADR 0002 deferred until this adapter existed. ADR 0002 already fixes the semantics: one outcome per `(order_id, operation)` in `payments`, and every provider call carries the idempotency key `<orderId>:<operation>`.

Nexus must never move real money (SPEC non-goals).

## Decision

1. **`StripePaymentGateway` implements `PaymentGateway`**, selected with `PAYMENT_GATEWAY=stripe`. The mock stays the default.
   - It calls Stripe's REST API directly with the JDK HTTP client and form encoding. No Stripe SDK: four endpoints don't justify a dependency, and the stub-server tests see exactly what goes over the wire.
   - **Authorize:** `POST /v1/payment_intents` with `capture_method=manual` and `confirm=true`. `requires_capture` means approved, and the PaymentIntent id becomes the authorization id stored in `payments.provider_ref`. A `card_error` is a decline (`INSUFFICIENT_FUNDS` or `CARD_DECLINED`). Any other status, such as `requires_action` for 3-D Secure, is a decline too: a backend saga has no customer to authenticate.
   - **Capture:** `/v1/payment_intents/{id}/capture`. **Void:** `/cancel`; "already canceled" counts as success. **Refund:** `POST /v1/refunds`; "already refunded" counts as success.
   - Every request sends the ADR 0002 idempotency key as Stripe's `Idempotency-Key` and pins `Stripe-Version`.
2. **Errors.**
   - Declines and refusals are **returned** as outcomes.
   - Timeouts, connection errors, 429 and 5xx **throw** `StripeUnavailableException`, so the command goes through the retry topics (ADR 0004). The retry sends the same key, so Stripe answers with its original result.
   - 401/403 (bad key) and idempotency conflicts throw `IllegalStateException`. They are not transient, but retrying lands them in the DLQ for replay after the fix.
3. **Circuit breaker** (Resilience4j, metrics via Micrometer): opens at 50% failures over 20 calls (minimum 10) and stays open for 30 s. Only `StripeUnavailableException` counts as a failure, so a run of declines never opens it. While it is open, calls fail fast with the same transient exception.
4. **Test mode only.**
   - The key comes from `STRIPE_API_KEY` and is never in code or config.
   - The adapter **refuses to start** unless the key matches `sk_test_…` or `rk_test_…`.
5. **`payment_attempts`: one row per provider call.** Recorded fields:
   - order and operation, provider, idempotency key;
   - outcome: `SUCCEEDED`, `DECLINED`, `FAILED` or `ERROR`;
   - reason, duration and start time.

   `ProviderCalls` wraps each call in `PaymentService` and writes the row in its **own transaction** (`REQUIRES_NEW`), so the row survives when the business transaction rolls back. That rollback case, a provider error or a crash after Stripe acted, is exactly where an audit trail matters. It also emits the metrics `payment_provider_calls_total{provider,operation,outcome}` and `payment_provider_call_seconds{provider,operation}`. Failing to write an attempt row is logged and never fails the payment.

## Alternatives considered

- **stripe-java SDK.** Typed models and built-in retries. But its retries would overlap ours, and it adds a large dependency for four calls. Rejected for now; easy to swap behind `PaymentGateway`.
- **Write attempts in the business transaction.** No second connection, but a rolled-back call would leave no trace. That makes the table a copy of `payments`. Rejected.
- **Two-phase local state** (a `PENDING` row, then the call outside the transaction). Still the ADR 0002 alternative, if connection hold time shows up in S1 with Stripe's real latency.

## Consequences

- Each provider call briefly needs a **second pooled connection** (the attempt row) while the first is held for the call (ADR 0002). Payment's Hikari pool is 20 so that every listener thread (main topic plus three retry topics) can do this at once. Raise it with listener concurrency.
- Stripe's PaymentIntent flow can't express `pm_mock_captureFails` / `pm_mock_refundFails`. Those failure tests stay on the mock.
- With Stripe, a fulfillment failure (refund after capture) creates a real test-mode charge plus refund, which is visible in the Stripe dashboard (SPEC step order).
- `payment_attempts` grows without bound. A retention job (like the outbox's 24 h cleanup) is a follow-up.
- Covered by:
  - `StripePaymentGatewayTest` (a stub Stripe server: requests, outcomes, transient errors, timeouts, breaker, key check);
  - `StripeGatewaySelectionIT` (wiring);
  - `PaymentAttemptsIT` (rows, including after a rollback);
  - `StripeLiveIT`, which runs against the real test API only when `STRIPE_API_KEY` is set. The adapter counts as done once that has passed.
