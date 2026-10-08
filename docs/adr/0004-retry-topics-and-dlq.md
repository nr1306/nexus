# ADR 0004 — Non-blocking retry topics, per-group DLQs and DLQ replay

- **Status:** Accepted
- **Date:** 2026-10-07
- **Scope:** `libs/messaging` (`retry`, `dlq`), every `@KafkaListener`

## Context

Until now, a failing message was retried a few times in place by Spring Kafka's default error handler and then **skipped**. That is data loss. It also blocked the partition while retrying, so one bad order could stall every other order on that partition. CLAUDE.md rule 10 asks for transient errors to go through retry topics (1 s, 5 s, 30 s) and then a DLQ, and for permanent errors to go straight to the DLQ. SPEC §7 asks for a replay operation.

## Decision

1. **Spring Kafka non-blocking retry topics**, auto-configured by `MessagingRetryAutoConfiguration` for every service that has `spring.kafka.consumer.group-id`. A failed record is republished to the next retry topic and the source partition moves on. The retry topic's consumer pauses that partition until the record is due. After the last delay the record goes to the DLQ.
2. **Exact delays.** `nexus.messaging.retry.delays` defaults to `1s,5s,30s`. `FixedDelaysBackOffPolicy` yields exactly that sequence (exponential back-off can't), giving one retry topic per delay.
3. **Topic names include the consumer group:**
   ```
   <topic>.<group>.retry-1s | -5s | -30s
   <topic>.<group>.dlq
   ```
   e.g. `inventory.events.order-saga.dlq`. Spring Kafka shares retry topics between all groups that consume a topic, so without the group in the name Reconciliation (Phase 3, also reading `inventory.events`) would receive Order's retries and vice versa. This **refines SPEC §5's `<topic>.retry-*` / `<topic>.dlq` naming**.
4. **Classification.** `InvalidEnvelopeException` plus Spring's built-in fatal exceptions (deserialization, conversion, `ClassCastException`, …) are **permanent** and go straight to the DLQ. Everything else is **transient** and retried: timeouts, DB errors, lock conflicts, a lost `(order_id, operation)` race. Classification looks at the cause chain. Business rejections (out of stock, card declined) are not exceptions; they are replies.
5. **Headers survive.** Retry hops and the DLQ keep the original headers (`traceparent`, `id`, `eventType`) and add Spring's `kafka_exception-*` / `kafka_original-*` headers.
6. **DLQ partitions = 12** (`nexus.messaging.retry.partitions`), the same as source topics, because records keep their partition number on every hop. This **replaces SPEC §5's 3 DLQ partitions**.
7. **DLQ handling and replay.**
   - A DLQ listener (`DeadLetterRecorder`) logs each dead letter with `orderId`/`eventId` and counts `dlq_messages_total{topic}`.
   - `DlqAdmin`, exposed at `/admin/dlq`, lists pending messages and replays them. A message is pending if it is after the committed offset of the `<group>.dlq-replay` consumer group.
     - `GET /admin/dlq` lists this service's DLQ topics.
     - `GET /admin/dlq/{dlq}/messages` shows partition, offset, key, eventId, eventType, root exception and message.
     - `POST /admin/dlq/{dlq}/replay` republishes all pending messages to the source topic and advances the replay offset.
     - `POST /admin/dlq/{dlq}/replay/{partition}/{offset}` republishes one message and leaves the offset where it is.
   - Replay drops `kafka_*` / `retry_topic-*` headers, so the message starts a fresh retry cycle, and keeps everything else. Consumers are idempotent, so replaying something that was in fact processed is harmless.
   - Replay is infrastructure, not business code, so its `KafkaTemplate.send` doesn't break rule 1: no state change is coupled to it.
8. **Per-order release marker in Inventory** (`released_orders`, V13). Retry topics break per-order ordering: a `ReserveInventory` sitting in a retry topic can arrive after the saga already compensated with `ReleaseInventory`. Release now records the order, and any later reserve is rejected as `ALREADY_RELEASED` instead of creating a hold nothing would release. Payment already had the equivalent: a `VOID` row, even a `SKIPPED` one, blocks a late authorize.

## Alternatives considered

- **Blocking retries** (`DefaultErrorHandler` with back-off). Simple, but the partition stalls for up to 36 s per bad record, which is exactly the head-of-line blocking SPEC §7 says retry topics avoid. Rejected.
- **Hand-rolled retry routing.** Possible, but delayed redelivery (pause/resume per partition) is the hard part, and Spring Kafka already does it well. Rejected.
- **One shared DLQ per topic (SPEC naming).** Breaks as soon as a second group consumes the topic. Rejected.
- **Replay by resetting the main group's offsets.** Re-processes everything, not just the failures. Rejected.

## Consequences

- Each consumed topic gains 4 extra topics per group (3 retry + DLQ), auto-created at startup.
- The full retry chain (≈36 s) is longer than the saga step timeout (30 s). A command stuck in retries is therefore re-sent by Order once. That is harmless, because receivers are idempotent per order, but it is visible in logs and metrics.
- `/admin/dlq` has no authentication. It must stay internal: the gateway (Phase 3) must not route `/admin`, and Kubernetes network policy should restrict it in Phase 5.
- Covered by `RetryAndDlqIT` (delays, names, headers, permanent vs transient, replay) and by `InventoryKafkaIT` (a poison message goes to the DLQ without blocking the order's next command).
