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
| Fulfillment | RefundPayment, ReleaseInventory |

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
contracts/            Protobuf (gRPC) + JSON Schemas for all events — source of truth for messages
libs/messaging/       Outbox writer, idempotent-consumer base class, retry/DLQ config
libs/observability/   Shared metrics + tracing setup
services/<name>/      One module per service (gateway is a Go module)
deploy/compose/       Local stack: Kafka, Postgres, Redis, Debezium, Keycloak, OpenSearch, Grafana
deploy/helm/          One chart per service + umbrella chart
deploy/connect/       Debezium, OpenSearch sink, S3 sink connector configs
infra/terraform/      vpc, eks, msk, rds, redis, opensearch, ecr, s3, iam, budget
bench/                gatling/, chaos/, scenarios/, results/
SPEC.md               Full project spec
docs/adr/             Architecture decision records
Makefile
```

## Commands

```bash
make up                         # start local stack (Docker Compose)
make down                       # stop local stack
make test                       # all unit + integration tests (Java + Go)
./gradlew :services:order:test  # one Java service
cd services/gateway && go test ./...
make bench SCENARIO=s1          # run a benchmark scenario (s1..s6), writes bench/results/*.json
make kind-up                    # local Kubernetes via kind + Helm
```

If a command does not exist yet, add it to the Makefile rather than documenting a one-off.

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
- Topics: `<service>.commands`, `<service>.events`, `order.events`, plus `<topic>.retry-1s|5s|30s` and `<topic>.dlq`.
- Any change to a message goes in `contracts/` first. Changes must be backward compatible; otherwise bump `schemaVersion` and handle both.
- Event types are past tense (`PaymentAuthorized`); commands are imperative (`AuthorizePayment`).

## Coding conventions

**Java**
- Package by feature, not by layer (`order.saga`, `order.api`, `order.persistence`).
- Constructor injection only; no field `@Autowired`.
- Money as `long` cents + ISO currency code; never `double`.
- All timestamps `Instant` in UTC.
- Flyway migrations in `src/main/resources/db/migration`; never edit an applied migration.
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

## Build phases (current status: not started)

1. **Core saga, local** — Order, Inventory, Payment, outbox + Debezium, compensations, idempotency, Testcontainers tests.
2. **Full flow + failure handling** — Fraud (gRPC + circuit breaker), Fulfillment/notifications, retry topics, DLQ + replay API, Stripe adapter.
3. **Edge, audit, reconciliation** — Go gateway, OpenSearch + S3 sinks, Reconciliation service.
4. **Observability + benchmarks** — OTel tracing across Kafka, dashboards, Gatling S1–S3, Chaos Mesh S4, S5–S6 scripts.
5. **AWS + CI/CD** — Terraform, Helm, KEDA, GitHub Actions, full benchmark runs on EKS.

Update the status line above when a phase's done criteria are met (see `SPEC.md` §12).

## When unsure

- Prefer the simplest design that keeps the non-negotiable rules intact.
- For any significant design decision, add a short ADR in `docs/adr/` (context, decision, alternatives, consequences).
- Don't add new languages, databases or infrastructure components without asking first — scope control matters more than features.
