# ADR 0011 — Trace propagation: one order, one trace

- **Status:** Accepted
- **Date:** 2026-10-10
- **Scope:** `libs/observability`; every Java service; Order's saga (`V12__saga_traceparent.sql`)

## Context

SPEC §9: one order is one trace, from the API call through every Kafka hop to the final event. Services never publish to Kafka directly; Debezium does, from the outbox (ADR 0001). So the trace can't ride on a producer's context. The outbox already has a `traceparent` column, which Debezium copies to the `traceparent` header. Until now nothing filled it.

Some saga work starts with no message or request to carry a trace: Order's timeout sweeper and fraud poller, and the fraud check that runs on an executor after commit.

No trace backend (Tempo/collector) is in the local stack yet. That's Phase 4 infrastructure, and adding it needs approval.

## Decision

1. **`libs/observability`:** Micrometer Tracing on the OpenTelemetry bridge, W3C propagation, sampling 100% locally (`TRACING_SAMPLING_PROBABILITY`).
   - **Export:** OTLP is wired but **off** (`TRACING_EXPORT_ENABLED=false`, `OTEL_EXPORTER_OTLP_ENDPOINT`) until a collector exists. Propagation and log correlation work without it.
2. **Outbox:** each outbox row gets the traceparent of the span that writes it (`TraceparentSource`, which replaces libs/messaging's no-op).
3. **Kafka:** `spring.kafka.listener.observation-enabled=true`. Each consumed record continues the trace from its header, and that includes the retry-topic containers.
4. **gRPC:** Micrometer's observation interceptors on the Order→Fraud client and on both gRPC servers.
5. **Saga:** `saga_instances.traceparent` stores the trace the order started in. The sweeper and the fraud check run inside `TraceContinuation.inTrace(...)`, a new span in that trace, so the commands they send join it.
6. **Logs:** ECS logs gain `traceId` / `spanId` next to `orderId` / `sagaId`.

## Alternatives considered

- **Instrument Debezium / Kafka Connect.** The header copy already carries the context; nothing more is needed on the relay.
- **OpenTelemetry Java agent.** Zero code, but it can't know that a sweeper's work belongs to an order's trace. It also adds a runtime agent to every image.
- **Context propagation onto the fraud executor.** That covers the post-commit trigger but not the poller or the sweeper; the stored traceparent covers all three.

## Consequences

- Verified by `SagaE2eIT`: every outbox row any service wrote for a completed order (Order, Inventory, Payment, Fulfillment) has the same trace id. `TraceContinuationTest` covers continuation against the real bridge.
- Spring Boot disables tracing in tests by default. The beans fall back to no-op, and integration tests see no traceparent.
- A span is created per consumed record and per gRPC call; at 100% sampling this costs CPU, which S1 will measure. Lower the probability for load tests if it shows.
- Viewing traces needs Tempo (or another OTLP backend) plus Grafana in compose: a Phase 4 step that needs approval.
