# ADR 0008 — Crash recovery: faster consumer handover, no saga timeouts before replies are read

- **Status:** Accepted
- **Date:** 2026-10-08
- **Scope:** Kafka consumer settings of every Kafka service; Order's timeout sweeper; the Phase 2 done check

## Context

SPEC §12's Phase 2 done criterion is that killing any one service mid-load loses no orders once it restarts. `PhaseTwoDoneCheckIT` checks this. It places orders at a steady rate and SIGKILLs each application service in turn (inventory, payment, fraud, fulfillment, order). Each killed service stays down for 10 s and is then restarted.

A first local trial (not a recorded result) lost no orders and kept every stock and money invariant. But some orders ended `CANCELLED (TIMEOUT_CREATE_SHIPMENT)` even though Fulfillment had created their shipments. The logs showed two causes.

1. **Slow handover.** A restarted service was healthy about 5 s after starting, but got its partitions back only after Kafka's session timeout. Kafka's default 45 s `session.timeout.ms` keeps a dead member's partitions assigned until the session expires.
2. **Sweeper starts before replies are read.** Order was killed while Fulfillment was still working through its backlog. After the restart, Order's timeout sweeper ran at once on deadlines that had expired during the outage. Order's own consumer received its partitions tens of seconds later, so some sagas hit their last attempt and compensated while their `ShipmentCreated` replies were waiting unread in Kafka.

Both outcomes were clean: the shipment was cancelled, the money refunded and the stock released. But they cancelled orders that had in fact succeeded.

## Decision

1. **`session.timeout.ms` = 10 s and `heartbeat.interval.ms` = 3 s** for every consumer in Order, Inventory, Payment and Fulfillment. A crashed instance's partitions move after about 10 s instead of 45 s. Graceful shutdowns are unaffected, because the member leaves the group explicitly.
2. **`ReplyConsumptionGate` in Order.**
   - The timeout sweeper does nothing until two things have happened: the reply listener (`SagaReplyListener`, through `ConsumerSeekAware`) has been assigned partitions of `inventory.events`, `payment.events` or `fulfillment.events`, and `nexus.order.timeout-sweeper.startup-grace` (10 s) has passed since then.
   - Until then, overdue sagas just wait. Their replies, if any, are applied first.
   - The gate opens once per process. Later rebalances don't close it again, because the step deadlines (30 s × attempts) are much longer than the new 10 s handover.

## Alternatives considered

- **Static group membership (`group.instance.id`).** A restarted pod would reclaim its partitions immediately. But every listener container in a service, including the main topic, each retry topic and the DLQ, needs its own stable instance id. Each pod also needs a stable identity, such as a StatefulSet ordinal. Too much machinery for Phase 2; revisit with Helm in Phase 5.
- **Longer step deadlines.** They would hide the race but slow down the reaction to real failures. Rejected.
- **Wait for zero consumer lag before allowing timeouts.** More precise, but the gate would need an admin client and lag tracking. Assignment plus a grace period covers the observed gap. Revisit if a large backlog after a restart outlasts the grace period.

## Consequences

- A JVM pause or network blip longer than 10 s now triggers a rebalance. Records being processed are redelivered, which is harmless because consumers are idempotent.
- Right after Order starts, saga timeouts are delayed by up to about 10 s plus the assignment time. A saga whose reply is truly lost compensates that much later.
- `make phase2-check` runs the done check and writes `bench/results/phase2-done-check-*.json`. Covered by `ReplyConsumptionGateTest`, `SagaTimeoutSchedulingTest` and `OrderKafkaIT` (gate opens on a real assignment).
