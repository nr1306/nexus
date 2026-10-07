# ADR 0001 — Transactional outbox relayed by Debezium

- **Status:** Accepted
- **Date:** 2026-10-07
- **Scope:** every service that publishes to Kafka (`libs/messaging`, `deploy/connect`)

## Context

Every saga step changes a service's database **and** tells another service about it via Kafka. Doing these as two separate writes (a "dual write") is unsafe: if the process dies, the network blips or Kafka is unavailable between the DB commit and the publish, the system either loses the message (state changed, nobody told) or publishes a message for state that was rolled back. Neither is acceptable for a saga that must end fully `COMPLETED` or fully `CANCELLED` (CLAUDE.md rule 1).

Kafka transactions don't solve this: they make Kafka writes atomic with each other, not with a Postgres commit.

## Decision

1. **Outbox table per service.** Business code writes the state change and an `outbox` row in one Postgres transaction, using `OutboxWriter.append(topic, aggregateType, envelope)`. `OutboxWriter` refuses to run without an active transaction. Business code never calls `KafkaTemplate.send`.
2. **Debezium relays the outbox.** One Debezium Postgres connector per service (`deploy/connect/outbox-connector.json`, `pgoutput`, filtered publication on `public.outbox`) reads committed rows from the WAL and the Outbox Event Router SMT publishes them. Only committed transactions reach the WAL stream, so rolled-back rows are never published.
3. **Schema** (shipped once in `libs/messaging` as `V1__outbox_and_processed_events.sql`):

   | Column | Purpose |
   |---|---|
   | `id uuid` PK | **Equals the envelope `eventId`.** Published as the `id` header; consumers dedupe on it. |
   | `aggregate_type text` | Informational (`order`, `reservation`, `payment`); not used for routing. |
   | `aggregate_id text` | `orderId`; the Kafka message key (rule 3). |
   | `event_type text` | Published as the `eventType` header. |
   | `topic text` | **Destination topic**, e.g. `inventory.commands`, `order.events`. Router uses `route.by.field=topic`, `route.topic.replacement=${routedByValue}`. |
   | `payload jsonb` | The full event envelope; published as the message value. |
   | `traceparent text` (nullable) | W3C trace context, published as the `traceparent` header. Filled by a `TraceparentSource` bean (no-op until Phase 4 OpenTelemetry). |
   | `created_at timestamptz` | For the 24 h cleanup job. |

4. **Consumers dedupe** with `IdempotentConsumer.handle(consumerGroup, envelope, sideEffect)`: it inserts `(consumer_group, event_id)` into `processed_events` and runs the side effect in the same transaction (rule 2). Delivery is at-least-once end to end; idempotency, not exactly-once, gives correctness.
5. **Migration numbering.** Versions V1–V9 are reserved for `libs/messaging`; service migrations start at V10. Flyway picks up the library's scripts from its jar on the classpath, so every service gets an identical outbox.

## Alternatives considered

- **Dual write (DB commit, then `KafkaTemplate.send`).** Simple, but loses or invents messages on failure. Rejected; it is the failure mode this project exists to avoid.
- **Polling publisher** (a scheduler reads unpublished outbox rows, sends them, marks them sent). No extra infrastructure, but adds polling latency and DB load, needs its own "sent" bookkeeping, and is harder to scale across replicas. Rejected in favour of log-based CDC.
- **Route by `aggregate_type`** (Debezium's default) storing the full topic name there. Avoids a column but overloads the meaning of `aggregate_type`, and one aggregate legitimately publishes to several topics (Order emits to `inventory.commands`, `payment.commands` and `order.events`). Rejected for an explicit `topic` column.
- **Separate outbox per topic.** More tables and connectors for no benefit. Rejected.
- **Add `traceparent` later.** Possible via a new migration, but every service would need it before Phase 4 tracing works; adding it now costs one nullable column.

## Consequences

- Kafka Connect + Debezium become required infrastructure locally (`make up`, `make connectors`) and in the cloud (Strimzi on EKS).
- Each service needs `wal_level=logical`, one replication slot and one publication. An inactive slot retains WAL, so a stopped connector must be monitored (Phase 4 alert) or removed.
- A connector can only be registered after the service has run its migrations (the filtered publication needs `public.outbox` to exist); `deploy/connect/register.sh` checks this and skips otherwise.
- The connector uses Debezium's default `initial` snapshot mode, so rows already in the outbox when a connector is first registered may be published again. That is safe because consumers are idempotent. (Not yet verified by a test; `OutboxDebeziumIT` only covers rows written after the slot is active.)
- Outbox rows are not deleted by the relay. A cleanup job deleting rows older than 24 h is still to be built.
- The end-to-end path is covered by `OutboxDebeziumIT`, which registers the same connector template against real Postgres, Kafka and Debezium Connect containers.
