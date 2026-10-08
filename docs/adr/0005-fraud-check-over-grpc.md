# ADR 0005 — Fraud check: synchronous gRPC step behind a circuit breaker

- **Status:** Accepted
- **Date:** 2026-10-07
- **Scope:** `services/fraud`, `contracts/proto/nexus/fraud/v1`, Order's `fraud` package and saga step 3

## Context

SPEC §3/§4 puts a fraud check between payment authorization and capture, called over gRPC (Order → Fraud) with a circuit breaker. If Fraud rejects the order, or the breaker stays open past the step deadline, the saga must void the payment and release stock. SPEC §14's open question on ordering was settled: (a), authorize → fraud → capture, as the current flow.

## Decision

1. **Fraud service** (Java, own `fraud_db`), using a plain grpc-java server managed by a Spring `SmartLifecycle`, with no gRPC starter. It also serves the standard gRPC health service and reflection, and keeps HTTP for actuator only.
   - **Rules** (`FraudRules`, pure functions): `AMOUNT_LIMIT` (total > 500,000 cents), `VELOCITY` (customer already has ≥ 5 evaluated orders in the last 10 min), `BLOCKLISTED_CUSTOMER`, `BLOCKLISTED_PAYMENT_METHOD`. Any rule that fires means REJECT, and every reason that fired is returned. The blocklist is seeded with `blocked-customer` and Stripe's test token `pm_card_radarBlock`.
   - **Idempotent per order:** the first decision is stored (`decisions`, PK `order_id`) and returned for every repeat call, even if the facts have changed since. Concurrent first calls race on the primary key, and the loser returns the winner's row.
   - Fraud has no Kafka or outbox. It only answers calls, so it doesn't use `libs/messaging`.
2. **Contract** in `contracts/proto/nexus/fraud/v1/fraud.proto`. The new Gradle module `:contracts` generates Java stubs. grpc-java 1.84 pairs with protobuf 3.25.x, so protoc and the runtime are pinned to the same 3.25.9.
3. **Order calls Fraud outside any transaction.** Entering `FRAUD_CHECK` is an ordinary saga transition: the state is saved, but no outbox command is written. It publishes a `FraudCheckRequested` event, which `FraudCheckRunner` handles **after commit** on a small executor. The runner calls Fraud, then applies the verdict in a short transaction that locks the saga row and re-checks that it is still awaiting `FRAUD_CHECK` for the same saga. No DB lock or connection is held during the network call.
4. **Failure handling.** The client uses a 2 s per-call deadline and a Resilience4j breaker: 50% failures over 20 calls (minimum 10) open it for 10 s, with metrics bound to Micrometer. Failures leave the saga in `FRAUD_CHECK`, and a poller (`nexus.order.fraud.poll-interval`, 1 s) retries. If no verdict arrives by the step deadline (30 s), the timeout sweeper compensates with reason `TIMEOUT_FRAUD_CHECK`. The fraud step has no undo, so the plan is `VoidPayment, ReleaseInventory`. A rejection compensates the same way with reason `FRAUD_REJECTED`.
5. **Saga changes.** `FORWARD = reserve → authorize → FRAUD_CHECK → capture`, and the new state is `FRAUD_APPROVED`. The migration V11 also adds `PAYMENT_CAPTURED` and `FULFILLING` for the fulfillment slice. Kafka replies that arrive while `FRAUD_CHECK` is awaited are ignored, because no reply type matches a synchronous step.

## Alternatives considered

- **Call Fraud inside the reply transaction.** Simplest, but it holds the saga row lock and a DB connection for up to the call deadline. When Fraud is down, every `PaymentAuthorized` reply would fail and churn through retry topics into the DLQ instead of waiting for the step deadline. Rejected.
- **Make Fraud a Kafka command/reply service** like the others. Uniform, but SPEC chooses gRPC deliberately: a fast synchronous decision, and a way to demonstrate gRPC plus a circuit breaker. Rejected.
- **Fail open** (approve when Fraud is unavailable). This trades fraud risk for availability; SPEC specifies fail-closed after the deadline. Rejected.
- **gRPC Spring starters** (net.devh, Spring gRPC). The first is unmaintained and the second is pre-1.0. A ~60-line lifecycle class is enough. Rejected.

## Consequences

- The runner's poller runs on every Order pod, so with several replicas Fraud may be asked about the same order more than once. This is harmless because Evaluate is idempotent per order, and a verdict is applied only while the saga still awaits it.
- With Fraud down, orders wait in `PAYMENT_AUTHORIZED` with a card hold for up to 30 s before compensating. The breaker keeps the poller from hammering a dead Fraud.
- Load generators must spread orders across many customers, or the velocity rule rejects them. The e2e tests use a fresh customer per order.
- Covered by `FraudRulesTest`, `FraudGrpcIT` (real gRPC + Postgres), `GrpcFraudClientTest` (deadline, breaker opening and failing fast, against an in-process server), `SagaIT` (reject, outage recovered, outage past deadline, late verdict) and `SagaE2eIT` (blocklisted card, a short Fraud outage with real containers).
