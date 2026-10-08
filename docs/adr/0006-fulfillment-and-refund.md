# ADR 0006 — Fulfillment as saga step 5, refund compensation and notifications

- **Status:** Accepted
- **Date:** 2026-10-07
- **Scope:** `services/fulfillment`, Payment's refund, Order's saga steps 5–6, `contracts/events/fulfillment`

## Context

SPEC §4 adds a fifth saga step after capture: Fulfillment creates the order's shipment. If that step fails, the saga must give the money back (`RefundPayment`) and release the stock. Until now, capture had no undo, so a capture timeout where the capture had actually happened ended in `NEEDS_ATTENTION` (ADR 0003).

SPEC §4 also says notifications are not saga steps: Fulfillment sends order confirmation, cancellation and shipping messages asynchronously, driven by `order.events`.

Retry topics reorder messages per order (ADR 0004). Any new consumer must therefore cope with a command that arrives after the compensation meant to undo it.

## Decision

1. **Fulfillment service** (Java, own `fulfillment_db`, port 8105), built on `libs/messaging`.
   - **`shipments`:** one row per order, primary key `order_id`. Status is `CREATED`, `SHIPPED`, `CANCELLED` or `REJECTED`. Every outcome is stored and replies are built from the row, so a retried command gets the same answer. This is the pattern ADR 0002 uses for payments.
   - **`CreateShipment`** (payload: `customerId`, `items`) inserts a `CREATED` row and replies `ShipmentCreated`. If an item is in `restricted_skus`, it stores `REJECTED` and replies `FulfillmentFailed(RESTRICTED_SKU)`. This is the step's business failure. The seeded `SKU-HAZMAT-01` is also stocked in Inventory's catalog, so local runs and e2e tests can trigger the failure.
   - **`CancelShipment`** cancels a `CREATED` shipment and replies `ShipmentCancelled(cancelled=true)`.
     - With no row, it inserts a `CANCELLED` marker and replies `cancelled=false`. A late `CreateShipment` then gets `FulfillmentFailed(ORDER_CANCELLED)`. This is the same idea as Inventory's `released_orders` and Payment's `VOID` row.
     - A `SHIPPED` shipment can't be cancelled; the reply is `ShipmentCancelFailed(ALREADY_SHIPPED)`.
   - **Dispatch happens on `OrderCompleted`, not on create.** The shipment is marked `SHIPPED` with a tracking number and `ShipmentShipped` is published. Nothing ships for an order whose saga might still compensate. As a result, `ALREADY_SHIPPED` can only happen if something is badly wrong, and Order then ends in `NEEDS_ATTENTION`.
   - **Notifications:** `OrderCompleted` → `ORDER_CONFIRMED` + `ORDER_SHIPPED`; `OrderCancelled` → `ORDER_CANCELLED`, including for orders that never had a shipment.
     - `notifications` is unique on `(order_id, type)`. Only the transaction that inserts the row sends the message, so a redelivery never notifies twice.
     - The channel is simulated: `LoggingNotificationSender` writes a log line. The metric is `notifications_sent_total{type}`.
     - Each notification has a stable id (UUID of `orderId:type`), which a real provider can use as an idempotency key. Without one, a rolled-back transaction after a real send could send again.
   - **One consumer group, `fulfillment-svc`**, reads both `fulfillment.commands` and `order.events`. SPEC §5 had planned a separate `fulfillment-notify` group. The retry/DLQ auto-configuration binds one group per service (ADR 0004), and one group keeps a single `processed_events` namespace and a single DLQ admin. The cost: a notification backlog shares consumer threads with saga commands. This **refines SPEC §5**.
2. **Refund in Payment.**
   - **`RefundPayment` → `PaymentRefunded` / `PaymentRefundFailed`.** It refunds the captured amount in full, at most once per order, with idempotency key `<orderId>:REFUND`. With no successful capture, it stores `SKIPPED` and replies `refunded=false`.
   - **Error handling:** a refund the provider refuses is stored `FAILED`, and the saga goes to `NEEDS_ATTENTION`. The mock refuses for `pm_mock_refundFails`. Provider errors are thrown and retried.
   - **A late capture is rejected:** once **any** `REFUND` row exists, even a `SKIPPED` one, `CapturePayment` fails with `ORDER_CANCELLED`, so a capture delayed in a retry topic can't charge the card after the refund step ran.
   - **`VoidPayment` after a refund is a no-op:** after capture + refund, it stores `SKIPPED` and replies `voided=false`. Without a refund it still fails with `ALREADY_CAPTURED`.
3. **Saga changes in Order.**
   - **The new flow:** `FORWARD = reserve → authorize → fraud → capture → CREATE_SHIPMENT`. On `ShipmentCreated` the saga completes, sends `CommitInventory` and publishes `OrderCompleted`. `OrderCompleted` and `OrderCancelled` now also carry `customerId`, an additive change.
   - **States:** while a step is awaited, the state is normally the previous step's result. Fulfillment is the exception: the transaction that sends `CreateShipment` moves the state to **`FULFILLING`**. So `PAYMENT_CAPTURED` is a transition in the state machine but is never stored.
   - **Undo mapping:** `undo(CAPTURE_PAYMENT) = REFUND_PAYMENT` and `undo(CREATE_SHIPMENT) = CANCEL_SHIPMENT`. The authorization's `VoidPayment` stays in every plan after a refund. Payment makes it a no-op when the money was refunded. It still releases the hold when the capture never happened, which matters on a capture timeout.

   | Failure | Plan |
   |---|---|
   | `CaptureFailed` | `VoidPayment`, `ReleaseInventory` (unchanged) |
   | capture timeout | `RefundPayment`, `VoidPayment`, `ReleaseInventory` |
   | `FulfillmentFailed` | `RefundPayment`, `VoidPayment` (no-op), `ReleaseInventory` |
   | shipment timeout | `CancelShipment`, `RefundPayment`, `VoidPayment` (no-op), `ReleaseInventory` |

## Alternatives considered

- **Drop `VoidPayment` from the plan when the capture is known to have succeeded.** This matches SPEC's table (`RefundPayment, ReleaseInventory`) exactly, but it makes the plan depend on *how* the step failed. A capture timeout needs both refund and void anyway. A uniform plan plus a no-op void in Payment is simpler. Rejected.
- **No `CancelShipment` (SPEC's table has no undo for step 5).** A timed-out `CreateShipment` sitting in a retry topic would then create a shipment for a cancelled, refunded order. Rejected.
- **Dispatch on `CreateShipment`.** A shipment would leave the warehouse before the saga commits. Rejected.
- **Separate notification service.** SPEC already merged it into Fulfillment. Rejected.
- **Make the notification group separate now** by teaching `libs/messaging` per-listener groups. It's possible, but it changes the retry naming and DLQ admin for every service. Revisit if notification volume ever delays saga commands.

## Consequences

- The "capture has no undo" risk is gone. A capture timeout now ends `CANCELLED` with the money refunded, not `NEEDS_ATTENTION`. This supersedes the capture note in ADR 0003 §5.
- A compensated fulfillment failure costs the customer a capture and a refund; with Stripe both appear on the statement. Real shops avoid this by checking shippability before capture. Here the step order follows SPEC.
- Order consumes `fulfillment.events` and logs and ignores `ShipmentShipped`, because the saga is already `COMPLETED`. Reconciliation (Phase 3) is its real consumer.
- **Covered by these tests:**
  - **Payment (`PaymentIT`):** refund once, refund duplicate, refund without capture blocking a late capture, void after refund, refused refund.
  - **Fulfillment:** `FulfillmentIT` covers create, duplicates, concurrent creates, restricted SKU, cancel, cancel-before-create, dispatch with notifications once, cancel after shipped, and the cancellation notice. `FulfillmentKafkaIT` runs the same flow over Kafka and Debezium.
  - **Order:** `SagaIT` covers happy path, fulfillment failure, shipment timeout, capture timeout and refused refund. `CompensationPlanTest` checks the plans above.
  - **End to end (`SagaE2eIT`):** the shipment is dispatched with notifications; a restricted SKU leads to refund, stock release and a cancellation notice.
