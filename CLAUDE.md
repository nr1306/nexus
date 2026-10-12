# CLAUDE.md — Nexus

Guidance for Claude Code in this repository. Read this fully before making changes. The full design lives in `SPEC.md`; read the relevant section before working on a service.

## What this project is

Nexus is an **event-driven, distributed order-processing backend** for a simulated e-commerce platform. Seven microservices drive each order through an **orchestrated saga** over Kafka until it is either fully `COMPLETED` or fully `CANCELLED`, and the system must stay correct under crashes, duplicate messages, races and traffic spikes.

It is a portfolio project. Its purpose is to demonstrate distributed-systems engineering with **real, reproducible benchmark numbers**. There is no customer-facing UI and no real money (Stripe test mode + a mock adapter).

## Architecture at a glance

```
Client --REST--> Gateway (Go) --gRPC--> Order (saga orchestrator) --gRPC--> Fraud
                                             |
                                       Kafka (MSK)  <-- Debezium outbox relay (Kafka Connect)
                                             |
                 Inventory   Payment   Fulfillment(+notifications)   Reconciliation
Kafka Connect sinks: OpenSearch (audit), S3 (event archive)
```

| Service | Lang | Responsibility |
|---|---|---|
| gateway | Go | JWT validation (Keycloak), Redis token-bucket rate limit, Idempotency-Key passthrough, REST→gRPC |
| order | Java | Order API, saga state machine, step deadlines, compensations |
| inventory | Java | Stock reservations with TTL, oversell prevention, Redis read cache |
| payment | Java | Authorize / capture / void / refund via Stripe test mode or mock adapter |
| fraud | Java | Rules engine (velocity, amount, blocklist) over gRPC; APPROVE / REJECT |
| fulfillment | Java | Shipment lifecycle + idempotent notifications |
| reconciliation | Java | Projects all events per order, checks invariants, issues repair commands |

### Saga (happy path)
`PENDING → INVENTORY_RESERVED → PAYMENT_AUTHORIZED → FRAUD_APPROVED → PAYMENT_CAPTURED → FULFILLING → COMPLETED`

Failure → `COMPENSATING` (undo finished steps in reverse) → `CANCELLED`, or `NEEDS_ATTENTION` if a compensation exhausts retries.

| Failure at | Compensation |
|---|---|
| Reserve stock | none |
| Authorize payment | ReleaseInventory |
| Fraud reject / capture fail | VoidPayment, ReleaseInventory |
| Fulfillment | RefundPayment, VoidPayment (no-op after refund), ReleaseInventory |

The table covers business failures. On a **timeout** the saga also undoes the in-flight step (it may have succeeded unheard), e.g. a reserve timeout sends `ReleaseInventory`, a shipment timeout sends `CancelShipment` first. See ADR 0003 and 0006. While step 5 is awaited the state is `FULFILLING` (`PAYMENT_CAPTURED` is never stored).

> **Open decision:** fraud check currently runs *after* payment authorization. Moving it before authorization is under consideration — see `SPEC.md` §14. Don't change the order without an ADR.

## Tech stack

- **Java 21 LTS, Spring Boot 3.x**, Spring Kafka, Resilience4j, Gradle (multi-module)
- **Go** for the gateway (net/http or chi, go-redis, grpc-go)
- **Kafka** (KRaft locally, AWS MSK in cloud), **Debezium** on Kafka Connect (Strimzi on EKS)
- **PostgreSQL 16** (one database per service) + Flyway, **Redis**, **OpenSearch**
- **gRPC + Protobuf** for Gateway→Order and Order→Fraud
- **OpenTelemetry, Prometheus, Grafana, Tempo**
- **Docker, Kubernetes, Helm, KEDA, Terraform (AWS), GitHub Actions**
- **JUnit 5, Testcontainers, Go testing, Gatling, Chaos Mesh**

## Repository layout

```
contracts/            Protobuf (gRPC, compiled by the `:contracts` Gradle module) + JSON Schemas for events — source of truth for messages
libs/messaging/       Outbox writer, idempotent-consumer base class, retry/DLQ config
libs/observability/   Tracing (Micrometer + OpenTelemetry bridge): traceparent on outbox rows, Kafka/gRPC propagation, TraceContinuation
services/<name>/      One module per service (gateway is a Go module)
deploy/compose/       Local stack: Kafka, Postgres, Redis, Debezium Connect (Keycloak, OpenSearch, Grafana planned)
deploy/helm/          One chart per service + umbrella chart
deploy/connect/       Debezium, OpenSearch sink, S3 sink connector configs
infra/terraform/      vpc, eks, msk, rds, redis, opensearch, ecr, s3, iam, budget
bench/                gatling/, chaos/, scenarios/, results/
SPEC.md               Full project spec
docs/adr/             Architecture decision records (0001 outbox, 0002 payment idempotency, 0003 saga, 0004 retry/DLQ, 0005 fraud, 0006 fulfillment/refund, 0007 reservation expiry, 0008 crash recovery, 0009 reconciliation, 0010 Stripe + payment attempts, 0011 trace propagation)
Makefile
```

## Commands

```bash
make up                         # start local stack (Kafka, Postgres, Redis, Debezium Connect); waits until healthy
make down                       # stop local stack (keeps data)
make clean                      # stop local stack and delete the Postgres volume
make connectors                 # register Debezium outbox connectors (each service must have run its migrations once)
make build                      # compile everything, skip tests
make test                       # all unit + integration tests (Java; Go added in Phase 3)
make e2e                        # end-to-end saga tests: every service as a container + Debezium (slow)
make phase1-check               # 1,000-order done check; writes bench/results/phase1-done-check-*.json
make phase2-check               # kill each service mid-load, 0 orders lost; writes bench/results/phase2-done-check-*.json
make s6-check                   # S6 reconciliation: 0 false positives, every injected inconsistency detected; writes bench/results/s6-reconciliation-*.json
make stripe-check               # Stripe adapter against the real test-mode API; needs STRIPE_API_KEY=sk_test_... in the environment
./gradlew :services:order:test  # one Java module
./gradlew :services:order:bootRun   # run one service against the local stack
```

Planned, not yet implemented: `cd services/gateway && go test ./...`, `make bench SCENARIO=s1` (writes `bench/results/*.json`), `make kind-up`.

If a command does not exist yet, add it to the Makefile rather than documenting a one-off.

### Local environment

- Gradle wrapper 8.14.5 runs on the default JDK 17; the build uses a **Java 21 toolchain** that Gradle downloads automatically (foojay). Don't point `JAVA_HOME` at Homebrew's JDK 27 (Gradle 8.14 can't run on it).
- Spring Boot **3.5.x**; versions live in `gradle/libs.versions.toml`.
- gRPC: grpc-java 1.84 with protoc/protobuf **3.25.9** (grpc-java's tested line; keep them equal). Fraud rejects `pm_card_radarBlock` and customer `blocked-customer` (seeded), totals > 500,000 cents, and a customer's 6th order within 10 min, so test/load data needs many customers.
- Mock payment methods (`MockPaymentGateway`): `pm_card_visa` approves, `pm_card_chargeDeclined` / `pm_card_chargeDeclinedInsufficientFunds` decline, `pm_mock_captureFails` authorizes but fails capture, `pm_mock_refundFails` captures but refuses the refund. `PAYMENT_GATEWAY=stripe` + `STRIPE_API_KEY=sk_test_…` switches to Stripe test mode (ADR 0010; live keys are refused, `pm_mock_*` don't exist there). Every provider call is logged in `payment_attempts`.
- Fulfillment refuses SKUs in its `restricted_skus` table. `SKU-HAZMAT-01` is seeded there and stocked in Inventory, so ordering it exercises fulfillment failure → refund. Shipments are dispatched (and the customer notified) only on `OrderCompleted`; notifications are log lines (`LoggingNotificationSender`).
- Place an order locally (after `make up`, starting the services, `make connectors`):
  `curl -X POST localhost:8101/orders -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' -d '{"customerId":"c1","currency":"USD","paymentMethod":"pm_card_visa","items":[{"sku":"SKU-0001","quantity":2,"unitPriceCents":1999}]}'`, then `GET localhost:8101/orders/<id>`. The REST API is on Order directly until the Go gateway (Phase 3) fronts it via gRPC; both call `OrderService`.
- Inventory serves `GET /stock/{sku}` (display only) through a Redis cache-aside (5 s TTL, evicted after each reserve/release commits; `STOCK_CACHE_ENABLED=false` turns it off). Redis is optional: errors fall back to Postgres.
- Ports: order **8101** (HTTP) / **9091** (gRPC, `nexus.order.v1.OrderService` for the gateway), inventory **8102**, payment **8103**, fraud **8104** (HTTP) / **9090** (gRPC), fulfillment **8105**, reconciliation **8106**; Kafka 9092, Postgres 5432 (`order_db`, `inventory_db`, `payment_db`, `fraud_db`, `fulfillment_db`, `reconciliation_db`; a Postgres volume created before a service existed lacks its database: run `make clean` once, or `CREATE DATABASE reconciliation_db` etc.), Redis 6379, Kafka Connect 8083.
- Container images used by tests must match `deploy/compose/docker-compose.yml` (`postgres:16`, `apache/kafka:4.1.2`, `quay.io/debezium/connect:3.7.0.Final`).
- Testcontainers is the Boot-managed 1.21.x. Don't add `debezium-testing-testcontainers` (it needs Testcontainers 2.x); run the Connect image as a `GenericContainer` instead.
- End-to-end tests live in `tests/e2e` and only run with `-Pe2e` (see Makefile). They run each service's bootJar in an `eclipse-temurin:21-jre` container, because services can't share one JVM (same `application.yml` path and `V10__` migrations). Service logs: `tests/e2e/build/e2e-logs/`.
- Integration tests: depend on `testImplementation(testFixtures(project(":libs:messaging")))` and use `NexusContainers` + `OutboxConnectors`. Follow `InventoryIntegrationTest`: one singleton Postgres/Kafka/Connect stack per test JVM, connector registered once, tests isolated by fresh SKUs/order ids instead of truncating.
- Structured logging: services use `logging.structured.format.console: ecs` and put `orderId`, `sagaId`, `eventId`, `eventType` in the MDC around message handling (see `InventoryCommandHandler`). Every Kafka-consuming service has this. `traceId`/`spanId` come from tracing (ADR 0011).
- Tracing: every Java service depends on `libs/observability`. One order is one trace: outbox rows carry `traceparent`, listeners continue it (`spring.kafka.listener.observation-enabled`), gRPC uses Micrometer's interceptors, and work without a message (sweepers, pollers) uses `TraceContinuation` with the trace stored on the saga. OTLP export is off (`TRACING_EXPORT_ENABLED`) until a collector is added. A new service or scheduled job acting on an order must keep this. Metrics: `libs/observability` brings the Prometheus registry, so every service serves `/actuator/prometheus` (SPEC §9 metrics).
- To stop a service you started, kill it by PID or by its port (`lsof -ti tcp:8101 | xargs kill`). **Never** use broad `pkill -f` patterns: on macOS they match every app under `/Applications`.

## Non-negotiable rules (correctness)

These are the point of the project. Never violate them, even to make a test pass.

1. **No dual writes.** A service must never write to its database and publish to Kafka separately. State change + outgoing event are written in **one DB transaction** (state tables + `outbox`). Debezium publishes from the outbox. Never call `KafkaTemplate.send` from business code.
2. **Every consumer is idempotent.** Insert `(consumer_group, event_id)` into `processed_events` in the **same transaction** as the side effect. If it already exists, skip. Use the shared base in `libs/messaging`.
3. **Every message is keyed by `orderId`.** This guarantees per-order ordering. Do not change keys.
4. **Each service owns its data.** Never read or write another service's tables. Cross-service information travels only via events, commands or gRPC.
5. **Oversell prevention is a conditional atomic update**: `UPDATE stock SET available = available - :qty, reserved = reserved + :qty WHERE sku = :sku AND available >= :qty`. Zero rows updated = reject. The Redis cache is read-only for availability display; writes always go to Postgres.
6. **Payments are unique per `(order_id, operation)`.** Authorize, capture, void, refund can each happen at most once per order.
7. **Compensations are idempotent and retried.** Releasing or refunding twice must be a no-op.
8. **Commit Kafka offsets only after the DB transaction commits.** Producers use `acks=all` and idempotence enabled.
9. **Saga state is persisted** in `saga_instances` before any command is emitted, so a restarted Order pod resumes correctly.
10. **Classify errors**: transient (timeouts, 5xx, lock conflicts) → retry topics (1s, 5s, 30s) → DLQ; permanent (schema/validation) → DLQ immediately.

## Event conventions

- Envelope on every message: `eventId` (UUID), `eventType`, `schemaVersion`, `orderId`, `sagaId`, `occurredAt` (UTC ISO-8601), `payload`.
- W3C `traceparent` travels as a Kafka header — never drop headers when re-publishing (retries, DLQ replay).
- Topics: `<service>.commands`, `<service>.events`, `order.events`, plus `<topic>.<group>.retry-1s|5s|30s` and `<topic>.<group>.dlq` (group in the name so groups never share retries; ADR 0004).
- Any change to a message goes in `contracts/` first. Changes must be backward compatible; otherwise bump `schemaVersion` and handle both.
- Event types are past tense (`PaymentAuthorized`); commands are imperative (`AuthorizePayment`).
- The envelope `eventId` **is** the outbox row `id`; Debezium publishes it as the `id` header and consumers dedupe on it.
- The destination topic is chosen per row by the outbox `topic` column (ADR 0001).

### Messaging API (`libs/messaging`)

- Publish: `OutboxWriter.append(topic, aggregateType, envelope)` inside the `@Transactional` method that changes state. It throws if there's no active transaction.
- Consume: wrap every side effect in `IdempotentConsumer.handle(consumerGroup, envelope, sideEffect)`. It returns `false` and increments `duplicate_events_skipped_total{consumer_group}` on a duplicate.
- Parse incoming values with `EnvelopeMapper.fromJson`. `InvalidEnvelopeException` is a permanent error (straight to DLQ).
- Retry topics + DLQ are auto-configured for every `@KafkaListener` (needs `spring.kafka.consumer.group-id`; listener `groupId` must equal it, so one consumer group per service: Fulfillment reads commands and `order.events` in `fulfillment-svc`, ADR 0006). Throw for transient failures; reply (don't throw) for business rejections. Tune with `nexus.messaging.retry.*`.
- DLQ admin on every service: `GET /admin/dlq`, `GET /admin/dlq/{dlq}/messages`, `POST /admin/dlq/{dlq}/replay[/{partition}/{offset}]`. Internal only.
- These beans are auto-configured in any service that depends on `libs/messaging`.

## Coding conventions

**Java**
- Package by feature, not by layer (`order.saga`, `order.api`, `order.persistence`).
- Constructor injection only; no field `@Autowired`.
- Money as `long` cents + ISO currency code; never `double`.
- All timestamps `Instant` in UTC.
- Flyway migrations in `src/main/resources/db/migration`; never edit an applied migration. **V1–V9 are reserved for `libs/messaging`** (outbox, processed_events); each service's own migrations start at **V10**.
- Micrometer for custom metrics; names as in `SPEC.md` §9 (`saga_completed_total`, `saga_compensated_total{reason}`, `duplicate_events_skipped_total`, …).

**Go (gateway)**
- Layout: `cmd/gateway`, `internal/...`.
- Rate limiting via a single atomic Redis Lua script; return 429 with `Retry-After`.
- Propagate context and trace headers on every outbound call.
- `go vet` and `golangci-lint` must pass.

**General**
- Small, focused PRs per build-plan item. Conventional commits (`feat(order): ...`, `fix(payment): ...`).
- Structured JSON logs with `orderId`, `sagaId`, `eventId` on every line that concerns an order.
- No secrets in code or Helm values; env vars locally, Secrets Manager in AWS.

## Testing expectations

- Every consumer gets a **duplicate-delivery test** (same event twice → one side effect).
- Every saga step gets a **failure test** asserting the correct compensations ran and the final state is `CANCELLED`.
- Integration tests use **Testcontainers** (real Kafka + Postgres), not mocks, for anything touching messaging or SQL.
- Inventory gets a **concurrency test**: N parallel reservations on stock < N → stock never negative, exactly `stock` successes.
- Don't mark work done if tests fail or are skipped.

## Benchmarks — honesty rules

Benchmark numbers will appear on a résumé. Treat them as evidence.

- **Never invent, estimate, round up or "project" a metric.** Every number must come from a file in `bench/results/`.
- Each results JSON records: commit SHA, scenario, environment (instance types, node count, partitions, replicas), timestamp, raw measurements.
- Run each scenario 3× and report the median.
- Metric definitions (don't redefine silently):
  - *Orders/sec* = orders reaching `COMPLETED` per second (not POSTs accepted).
  - *End-to-end latency* = `saga_duration_seconds` (POST accepted → COMPLETED). Gateway latency reported separately.
  - *Recovery time* = fault injected → error rate and lag back to baseline.
  - *Compensation success* = clean `CANCELLED` / sagas that entered `COMPENSATING`.
- Scenarios: S1 steady ramp, S2 10× spike (KEDA on/off), S3 duplicate storm, S4 infra faults (Chaos Mesh), S5 business failures, S6 reconciliation.

## AWS and cost rules

- Day-to-day development is **local only** (Docker Compose / kind).
- **Never run `terraform apply` or `destroy` unless explicitly asked.** Always show `terraform plan` first.
- After any cloud benchmark session, remind to run `terraform destroy`.
- Prefer Strimzi Kafka on EKS during cloud iteration; use MSK only for final benchmark runs.
- An AWS Budgets alert module must exist before the first apply.
- CI authenticates to AWS via GitHub OIDC only — never long-lived access keys.

## Known risks to handle later

- **Payment provider calls hold a DB connection** for the call's duration (ADR 0002). Fine for the mock; measure with Stripe latency in S1.
- **Retry chain (≈36 s) > saga step timeout (30 s):** a command stuck in retries gets re-sent once by Order. Harmless (idempotent receivers) but visible.
- **Retry topics reorder messages per order.** Handled where it matters: Inventory's `released_orders` marker, Payment's `VOID`/`REFUND` rows and Fulfillment's `CANCELLED` shipment marker reject late reserves/authorizes/captures/creates. New consumers must think about the same.
- **Fraud outage can outlast the 30 s fraud deadline:** a failed half-open probe (gRPC reconnect backoff right after Fraud restarts) re-opens Order's breaker for 10 s, so an outage shorter than the deadline can still cancel a few in-flight orders with `TIMEOUT_FRAUD_CHECK` (seen intermittently in local Phase 2 trials; clean compensation, allowed by the check). Options: shorter `open-state-wait`, or a channel reset on restart.
- **`payment_attempts` takes a second connection per provider call** (own transaction, ADR 0010); Payment's pool is 20. The table has no retention job yet.
- **Fraud poller runs on every Order pod:** duplicate Evaluate calls for one order are possible with replicas; harmless (Fraud is idempotent per order, verdict applied only while awaited).
- **`/admin/dlq` is unauthenticated:** keep it off the gateway (Phase 3) and behind network policy (Phase 5).
- **Saga state CHECK:** a new saga state needs a **new** migration for the `saga_instances.state` CHECK (as V11 did); never edit an applied one.
- **Commit after an expired hold can oversell:** only if a saga outlives the 10 min TTL and the stock was sold meanwhile; reported as `InventoryCommitted.shortfall` + `inventory_commit_shortfall_total` for Reconciliation (Phase 3) (ADR 0007).
- **Notifications are sent inside the consumer transaction:** a rollback after a real send would resend. The log sender makes this moot; a real channel must use the stable notification id as its idempotency key (ADR 0006).
- **Fulfillment failures happen after capture**, so the customer sees a charge and a refund (SPEC step order). Revisit if the Stripe adapter makes this visible.
- **Saga timeouts wait for replies after an Order restart:** `ReplyConsumptionGate` holds the timeout sweeper until the reply listener is assigned partitions + `startup-grace` (10 s). Consumers use a 10 s `session.timeout.ms` so a crashed instance's partitions move quickly (ADR 0008). Static membership is the Phase 5 option.
- **Reconciliation only sees what was published** (ADR 0009): silent DB corruption with no event is out of reach. Invariant 1 (completed without capture/shipment) and 4 (stuck saga, `NEEDS_ATTENTION`) are flag-only until Order has an `order.commands` topic; invariant 5 (per-SKU reserved = held) is deferred until Inventory publishes a drift event. `/admin/findings` is unauthenticated, like `/admin/dlq`.
- **The e2e stack runs Reconciliation with short timings** (`E2eStack.RECONCILIATION_TIMINGS`: 5 s check, 30 s settle, 90 s stuck, 30 s repair retry), so its repairs can act during other e2e tests too. The settle window must exceed how late a follow-up command can be (a 10 s window flagged orders whose `CommitInventory` was delayed by a killed Inventory); the Phase 2 check asserts 0 findings.
- **Idempotency-Key is global**, not scoped per customer/API key. Scope it when the gateway (Phase 3) passes the caller identity.

## Build phases (current status: Phase 1 done — `bench/results/phase1-done-check-20261007T123719Z.json` @ 3a92c2f; Phase 2 in progress)

1. **Core saga, local** — Order, Inventory, Payment, outbox + Debezium, compensations, idempotency, Testcontainers tests.
2. **Full flow + failure handling** — Fraud (gRPC + circuit breaker), Fulfillment/notifications, retry topics, DLQ + replay API, Stripe adapter.
3. **Edge, audit, reconciliation** — Go gateway, OpenSearch + S3 sinks, Reconciliation service.
4. **Observability + benchmarks** — OTel tracing across Kafka, dashboards, Gatling S1–S3, Chaos Mesh S4, S5–S6 scripts.
5. **AWS + CI/CD** — Terraform, Helm, KEDA, GitHub Actions, full benchmark runs on EKS.

Update the status line above when a phase's done criteria are met (see `SPEC.md` §12).

**Phase 1 progress:**
- [x] Monorepo, Gradle, Docker Compose (Kafka, Postgres, Debezium, Redis)
- [x] Outbox + Debezium connector, idempotent consumer base (`libs/messaging`, ADR 0001)
- [x] Inventory: reserve (all-or-nothing) / release, oversell prevention, duplicate + concurrency tests
- [x] Completed orders settle their hold: Order sends `CommitInventory`; Inventory marks holds `COMMITTED` (never released or expired)
- [x] Payment: authorize / capture / void with stateless mock adapter, at most once per `(order_id, operation)` (ADR 0002)
- [x] Order: REST API + Idempotency-Key, saga reserve → authorize → capture → complete, compensations, deadlines (ADR 0003)
- [x] Saga tests per service (replies simulated): happy path, out-of-stock, card decline, capture failure, duplicates, timeouts
- [x] Cross-service end-to-end tests (`make e2e`): every service as a container + Debezium, incl. Payment outage + Order restart mid-saga
- [x] Done check (`make phase1-check`): 1,000 mixed-failure orders → 0 stock drift, 0 double charges (`bench/results/phase1-done-check-*.json`)

**Phase 2 progress:**
- [x] Retry topics (1 s/5 s/30 s) + per-group DLQ + DLQ admin/replay; Inventory release marker (ADR 0004)
- [x] Fraud service over gRPC + Resilience4j circuit breaker; saga step 3 (ADR 0005)
- [x] Fulfillment + notifications; refund compensation; saga step 5 (ADR 0006)
- [ ] Stripe test-mode adapter: built + stub-server tests, circuit breaker, test-key guard (ADR 0010); tick after `make stripe-check` passes with a real test key
- [x] `payment_attempts`: one row per provider call, kept on rollback (ADR 0010)
- [x] Redis stock read cache: `GET /stock/{sku}`, cache-aside, evicted after commit, metrics `stock_cache_*`
- [ ] Done check: killing any one service mid-load loses no orders once it restarts (`make phase2-check`; consumer handover + timeout gate, ADR 0008; needs a run from a clean commit)

- [x] Reservation expiry sweeper (SPEC §7): overdue holds → `EXPIRED`, stock returned, `ReservationExpired` makes a still-running saga compensate; a commit after expiry re-takes stock or reports a shortfall (ADR 0007)

**Phase 3 progress:**
- [x] Reconciliation service: event projection (facts), invariants 1–4, `RefundPayment` / `CommitInventory` / `ReleaseInventory` repairs via the outbox, `/admin/findings` (ADR 0009)
- [x] Order gRPC API for the gateway (`contracts/proto/nexus/order/v1/order.proto`: PlaceOrder, GetOrder; same `OrderService` and Idempotency-Key as REST)
- [ ] Done check, S6 (`make s6-check`): fault-free load → 0 findings; injected inconsistencies per type → detected, repairable ones fixed (passes locally; needs a run from a clean commit for `bench/results`)
- [ ] Go gateway: routing, JWT via Keycloak, Redis token bucket, gRPC to Order (needs Go installed)
- [ ] OpenSearch sink and S3 archive connectors (needs OpenSearch/MinIO in compose; ask first)
- Follow-ups: `order.commands` so stuck sagas can be resumed or compensated; invariant 5 via an Inventory drift event

**Phase 4 progress (started early):**
- [x] Trace propagation: one order = one trace across outbox → Debezium → Kafka, gRPC and Order's sweepers (ADR 0011; verified in `SagaE2eIT`)
- [x] `/actuator/prometheus` on every service (the registry was missing; SPEC §9 metrics are now scrapeable)
- [ ] Trace export + Tempo/Grafana/Prometheus in compose (needs approval: new infrastructure)


## When unsure

- Prefer the simplest design that keeps the non-negotiable rules intact.
- For any significant design decision, add a short ADR in `docs/adr/` (context, decision, alternatives, consequences).
- Don't add new languages, databases or infrastructure components without asking first — scope control matters more than features.
