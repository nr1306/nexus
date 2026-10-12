package com.nexus.e2e;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * S6 (SPEC.md §10) and the Phase 3 done criterion: every injected inconsistency is detected (ADR 0009 §7).
 *
 * <p>First a mixed load with no faults runs and must produce <b>no findings</b> (false-positive check).
 * Then, per finding type, a bug is simulated in the owning service: a faulty row plus the event it would
 * have published (an outbox row, so Debezium publishes it as the service would). The test records the
 * injection time and measures detection rate, time to detect and, for repairable types, whether the
 * repair command fixed the service's data. With {@code -PresultsDir=...} the measurements are written
 * as JSON. Correctness check with the shortened e2e timings ({@link E2eStack#RECONCILIATION_TIMINGS}).
 */
class ReconciliationCheckIT extends E2eSupport {

    private static final long SEED = 6;
    private static final int CLIENT_THREADS = 16;
    private static final String RESTRICTED_SKU = "SKU-HAZMAT-01";
    private static final String VISA = "pm_card_visa";
    private static final String DECLINED = "pm_card_chargeDeclined";
    private static final String CAPTURE_FAILS = "pm_mock_captureFails";
    private static final Duration SETTLE = Duration.parse(E2eStack.RECONCILIATION_TIMINGS.get("RECONCILIATION_SETTLE_WINDOW"));
    private static final Duration INTERVAL = Duration.parse(E2eStack.RECONCILIATION_TIMINGS.get("RECONCILIATION_CHECK_INTERVAL"));
    private static final Duration DETECT_DEADLINE = Duration.ofMinutes(3);
    private static final Duration REPAIR_DEADLINE = Duration.ofMinutes(2);

    /** One injected inconsistency; {@code expected} are the finding types it must raise. */
    private record Injection(String kind, UUID orderId, Instant injectedAt, Set<String> expected, Repair repair) {
    }

    /** Checks the owning service's data once the repair command has been applied; null = flag only. */
    @FunctionalInterface
    private interface Repair {
        boolean applied();
    }

    private final JdbcTemplate orderDb = E2eStack.db("order");
    private final JdbcTemplate inventoryDb = E2eStack.db("inventory");
    private final JdbcTemplate paymentDb = E2eStack.db("payment");
    private final JdbcTemplate reconciliationDb = E2eStack.db("reconciliation");

    @Test
    void everyInjectedInconsistencyIsDetectedAndNormalLoadRaisesNone() throws Exception {
        int orders = Integer.getInteger("nexus.doneCheck.orders", 200);
        int perType = Integer.getInteger("nexus.s6.injectionsPerType", 5);
        Instant started = Instant.now();

        // --- 1. Normal load: no findings ---
        String sku = newSku(1_000_000);
        Random random = new Random(SEED);
        ExecutorService clients = Executors.newFixedThreadPool(CLIENT_THREADS);
        List<Future<UUID>> futures = new ArrayList<>();
        Map<String, Integer> mix = new TreeMap<>();
        for (int i = 0; i < orders; i++) {
            int roll = random.nextInt(100);
            String paymentMethod = roll < 75 || roll >= 92 ? VISA : roll < 85 ? DECLINED : CAPTURE_FAILS;
            Map<String, Integer> items = roll >= 92 ? Map.of(sku, 1, RESTRICTED_SKU, 1) : Map.of(sku, 1 + random.nextInt(3));
            mix.merge(roll >= 92 ? "restrictedSku" : paymentMethod, 1, Integer::sum);
            futures.add(clients.submit(() -> placeOrder(paymentMethod, items)));
        }
        List<UUID> loadIds = new ArrayList<>();
        for (Future<UUID> f : futures) {
            loadIds.add(f.get());
        }
        clients.shutdown();
        UUID[] ids = loadIds.toArray(new UUID[0]);
        boolean loadTerminal = waitUntil(Duration.ofMinutes(5), () -> orderDb.queryForObject(
                "SELECT count(*) FROM saga_instances WHERE order_id = ANY (?) AND state NOT IN ('COMPLETED','CANCELLED','NEEDS_ATTENTION')",
                Long.class, (Object) ids) == 0);
        // Projected, quiet for the settle window, then two more check passes.
        boolean loadChecked = waitUntil(Duration.ofMinutes(3), () -> reconciliationDb.queryForObject(
                "SELECT count(*) FROM order_projection WHERE order_id = ANY (?) AND (completed_at IS NOT NULL OR cancelled_at IS NOT NULL) "
                        + "AND last_event_at < now() - ?::interval", Long.class, (Object) ids,
                (SETTLE.plus(INTERVAL.multipliedBy(2))).toSeconds() + " seconds") == ids.length);
        Map<String, Long> loadStates = countBy(orderDb.queryForList(
                "SELECT state FROM saga_instances WHERE order_id = ANY (?)", String.class, (Object) ids));
        Map<String, Long> falsePositives = countBy(reconciliationDb.queryForList(
                "SELECT finding_type FROM findings WHERE order_id = ANY (?)", String.class, (Object) ids));

        // --- 2. Injections ---
        List<Injection> injections = new ArrayList<>();
        for (int i = 0; i < perType; i++) {
            injections.add(completedWithoutCaptureOrShipment());
            injections.add(capturedAfterCancellation());
            injections.add(stockHeldAfterCancellation());
            injections.add(stockHeldAfterCompletion());
            injections.add(stuckSaga());
        }

        Map<UUID, Map<String, Instant>> detected = new LinkedHashMap<>();
        boolean allDetected = waitUntil(DETECT_DEADLINE, () -> {
            detected.clear();
            for (Injection injection : injections) {
                detected.put(injection.orderId(), detections(injection.orderId()));
            }
            return injections.stream().allMatch(inj -> detected.get(inj.orderId()).keySet().containsAll(inj.expected()));
        });
        boolean allRepaired = waitUntil(REPAIR_DEADLINE, () -> injections.stream()
                .filter(inj -> inj.repair() != null)
                .allMatch(inj -> inj.repair().applied() && resolved(inj.orderId(), inj.expected())));

        // --- Measurements per kind ---
        ObjectNode byKind = JSON.createObjectNode();
        ArrayNode raw = JSON.createArrayNode();
        long missed = 0;
        long unexpectedTypes = 0;
        long repairable = 0;
        long repaired = 0;
        for (String kind : injections.stream().map(Injection::kind).distinct().toList()) {
            List<Injection> ofKind = injections.stream().filter(inj -> inj.kind().equals(kind)).toList();
            List<Long> millis = new ArrayList<>();
            long kindDetected = 0;
            long kindRepaired = 0;
            for (Injection inj : ofKind) {
                Map<String, Instant> found = detections(inj.orderId());
                boolean ok = found.keySet().containsAll(inj.expected());
                Set<String> extra = new java.util.TreeSet<>(found.keySet());
                extra.removeAll(inj.expected());
                unexpectedTypes += extra.size();
                boolean fixed = inj.repair() != null && inj.repair().applied() && resolved(inj.orderId(), inj.expected());
                ObjectNode r = raw.addObject().put("kind", kind).put("orderId", inj.orderId().toString())
                        .put("injectedAt", inj.injectedAt().toString()).put("detected", ok);
                r.putPOJO("expectedTypes", inj.expected());
                r.putPOJO("unexpectedTypes", extra);
                if (ok) {
                    kindDetected++;
                    // Time to detect = last expected type's detected_at - injection commit time.
                    Instant at = inj.expected().stream().map(found::get).max(Instant::compareTo).orElseThrow();
                    long ms = Duration.between(inj.injectedAt(), at).toMillis();
                    millis.add(ms);
                    r.put("timeToDetectMs", ms);
                } else {
                    missed++;
                }
                if (inj.repair() != null) {
                    repairable++;
                    r.put("repaired", fixed);
                    if (fixed) {
                        kindRepaired++;
                        repaired++;
                    }
                }
            }
            ObjectNode k = byKind.putObject(kind);
            k.putPOJO("findingTypes", ofKind.getFirst().expected());
            k.put("injected", ofKind.size());
            k.put("detected", kindDetected);
            k.put("detectionRate", (double) kindDetected / ofKind.size());
            millis.sort(Long::compare);
            k.putPOJO("timeToDetectMs", millis);
            if (!millis.isEmpty()) {
                k.put("timeToDetectMedianMs", millis.get(millis.size() / 2));
                k.put("timeToDetectMaxMs", millis.getLast());
            }
            if (ofKind.getFirst().repair() != null) {
                k.put("repaired", kindRepaired);
                k.put("autoRepairRate", (double) kindRepaired / ofKind.size());
            } else {
                k.put("repair", "flag only (ADR 0009 §5)");
            }
        }

        Instant finished = Instant.now();
        writeResults(started, finished, orders, perType, mix, loadStates, loadTerminal, loadChecked, falsePositives,
                byKind, raw, injections.size(), missed, unexpectedTypes, repairable, repaired, allDetected, allRepaired);

        assertThat(loadTerminal).as("normal load reached terminal states").isTrue();
        assertThat(loadChecked).as("normal load projected and checked by Reconciliation").isTrue();
        assertThat(falsePositives).as("findings for the fault-free load (false positives)").isEmpty();
        assertThat(missed).as("injections not detected").isZero();
        assertThat(unexpectedTypes).as("findings of an unexpected type on injected orders").isZero();
        assertThat(repaired).as("repairable injections fixed by Reconciliation's repair command").isEqualTo(repairable);
    }

    // --- Injections (ADR 0009 §7): each simulates a service bug that published something wrong ---

    /** #1: Order publishes OrderCompleted for an order whose payment and shipment never happened. */
    private Injection completedWithoutCaptureOrShipment() {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        String customer = newCustomer();
        Instant at = inTransaction(orderDb, () -> {
            orderDb.update("INSERT INTO orders (id, customer_id, total_cents, currency, payment_method) VALUES (?, ?, 1000, 'USD', ?)",
                    orderId, customer, VISA);
            orderDb.update("INSERT INTO order_items (order_id, sku, quantity, unit_price_cents) VALUES (?, 'SKU-0001', 1, 1000)", orderId);
            orderDb.update("INSERT INTO saga_instances (saga_id, order_id, state) VALUES (?, ?, 'COMPLETED')", sagaId, orderId);
            outbox(orderDb, "order", "order.events", "OrderCreated", orderId, sagaId, orderCreated(customer));
            outbox(orderDb, "order", "order.events", "OrderCompleted", orderId, sagaId,
                    JSON.createObjectNode().put("totalCents", 1000).put("currency", "USD").put("customerId", customer));
        });
        return new Injection("completedWithoutCaptureOrShipment", orderId, at,
                Set.of("COMPLETED_NOT_CAPTURED", "COMPLETED_NOT_SHIPPED"), null);
    }

    /** #2: Payment records and publishes a capture for an order the saga cancelled (card declined). */
    private Injection capturedAfterCancellation() {
        UUID orderId = cancelledOrder();
        UUID sagaId = sagaId(orderId);
        Instant at = inTransaction(paymentDb, () -> {
            paymentDb.update("INSERT INTO payments (order_id, operation, status, amount_cents, currency, provider, provider_ref) "
                    + "VALUES (?, 'CAPTURE', 'SUCCEEDED', 1000, 'USD', 'mock', ?)", orderId, "s6-auth-" + orderId);
            outbox(paymentDb, "payment", "payment.events", "PaymentCaptured", orderId, sagaId,
                    JSON.createObjectNode().put("amountCents", 1000).put("currency", "USD"));
        });
        return new Injection("capturedAfterCancellation", orderId, at, Set.of("CANCELLED_BUT_CHARGED"),
                () -> payments(orderId).contains("REFUND:SUCCEEDED"));
    }

    /** #3: Inventory takes and publishes a hold for an order that was already cancelled and released. */
    private Injection stockHeldAfterCancellation() {
        UUID orderId = cancelledOrder();
        String sku = injectHold(orderId);
        return new Injection("stockHeldAfterCancellation", orderId, injectedAt(orderId), Set.of("STOCK_HELD_AFTER_CANCELLATION"),
                () -> reserved(sku) == 0 && available(sku) == 10);
    }

    /**
     * #3, completed: Inventory takes and publishes a hold for an order that completed and committed. The
     * repair commits it (the stock is sold); a release would put sold units back on sale.
     */
    private Injection stockHeldAfterCompletion() {
        UUID orderId = finishedOrder(VISA, "COMPLETED", "completed_at IS NOT NULL AND committed_at IS NOT NULL");
        String sku = injectHold(orderId);
        return new Injection("stockHeldAfterCompletion", orderId, injectedAt(orderId), Set.of("STOCK_HELD_AFTER_COMPLETION"),
                () -> reserved(sku) == 0 && available(sku) == 9 && "COMMITTED".equals(inventoryDb.queryForObject(
                        "SELECT status FROM reservations WHERE order_id = ? AND sku = ?", String.class, orderId, sku)));
    }

    private final Map<UUID, Instant> holdInjectedAt = new java.util.concurrent.ConcurrentHashMap<>();

    private Instant injectedAt(UUID orderId) {
        return holdInjectedAt.get(orderId);
    }

    /** A HELD reservation of 1 unit of a fresh SKU (stock 10) plus its InventoryReserved; returns the SKU. */
    private String injectHold(UUID orderId) {
        UUID sagaId = sagaId(orderId);
        String sku = newSku(10);
        Instant expiresAt = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.MILLIS);
        Instant at = inTransaction(inventoryDb, () -> {
            assertThat(inventoryDb.update("UPDATE stock SET available = available - 1, reserved = reserved + 1 WHERE sku = ?", sku))
                    .isEqualTo(1);
            inventoryDb.update("INSERT INTO reservations (order_id, sku, quantity, status, expires_at, saga_id) VALUES (?, ?, 1, 'HELD', ?, ?)",
                    orderId, sku, Timestamp.from(expiresAt), sagaId);
            ObjectNode payload = JSON.createObjectNode().put("expiresAt", expiresAt.toString());
            payload.putArray("items").addObject().put("sku", sku).put("quantity", 1);
            outbox(inventoryDb, "reservation", "inventory.events", "InventoryReserved", orderId, sagaId, payload);
        });
        holdInjectedAt.put(orderId, at);
        return sku;
    }

    /** #4: Order starts a saga but never sends its first command, and its deadline is far away. */
    private Injection stuckSaga() {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        String customer = newCustomer();
        Instant at = inTransaction(orderDb, () -> {
            orderDb.update("INSERT INTO orders (id, customer_id, total_cents, currency, payment_method) VALUES (?, ?, 1000, 'USD', ?)",
                    orderId, customer, VISA);
            orderDb.update("INSERT INTO order_items (order_id, sku, quantity, unit_price_cents) VALUES (?, 'SKU-0001', 1, 1000)", orderId);
            orderDb.update("INSERT INTO saga_instances (saga_id, order_id, state, step, step_deadline, step_attempts) "
                    + "VALUES (?, ?, 'PENDING', 'RESERVE_INVENTORY', now() + interval '1 day', 1)", sagaId, orderId);
            outbox(orderDb, "order", "order.events", "OrderCreated", orderId, sagaId, orderCreated(customer));
        });
        return new Injection("stuckSaga", orderId, at, Set.of("SAGA_STUCK"), null);
    }

    /** A real order cancelled by a card decline: reserved, released, never charged. */
    private UUID cancelledOrder() {
        return finishedOrder(DECLINED, "CANCELLED", "cancelled_at IS NOT NULL AND released_at IS NOT NULL");
    }

    /**
     * Places a real order and waits until it ended in {@code state} and Reconciliation has projected
     * {@code projected}, so the faulty event lands afterwards, as a later bug would.
     */
    private UUID finishedOrder(String paymentMethod, String state, String projected) {
        UUID orderId = placeOrder(paymentMethod, Map.of("SKU-0001", 1));
        assertThat(awaitTerminal(orderId)).isEqualTo(state);
        assertThat(waitUntil(Duration.ofMinutes(1), () -> reconciliationDb.queryForObject(
                "SELECT count(*) FROM order_projection WHERE order_id = ? AND " + projected, Long.class, orderId) == 1))
                .as(state + " projected").isTrue();
        return orderId;
    }

    private ObjectNode orderCreated(String customer) {
        ObjectNode payload = JSON.createObjectNode().put("customerId", customer).put("totalCents", 1000).put("currency", "USD");
        payload.putArray("items").addObject().put("sku", "SKU-0001").put("quantity", 1).put("unitPriceCents", 1000);
        return payload;
    }

    /** Appends an event the way {@code OutboxWriter} does: the envelope is the payload, its eventId the row id. */
    private static void outbox(JdbcTemplate db, String aggregateType, String topic, String eventType, UUID orderId,
                               UUID sagaId, ObjectNode payload) {
        UUID eventId = UUID.randomUUID();
        ObjectNode envelope = JSON.createObjectNode()
                .put("eventId", eventId.toString())
                .put("eventType", eventType)
                .put("schemaVersion", 1)
                .put("orderId", orderId.toString())
                .put("sagaId", sagaId.toString())
                .put("occurredAt", Instant.now().toString());
        envelope.set("payload", payload);
        db.update("INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload) VALUES (?, ?, ?, ?, ?, ?::jsonb)",
                eventId, aggregateType, orderId.toString(), eventType, topic, envelope.toString());
    }

    /** Runs the statements in one transaction on the service's database; returns the commit time. */
    private static Instant inTransaction(JdbcTemplate db, Runnable statements) {
        var tx = new org.springframework.transaction.support.TransactionTemplate(
                new org.springframework.jdbc.datasource.DataSourceTransactionManager(db.getDataSource()));
        tx.executeWithoutResult(status -> statements.run());
        return Instant.now();
    }

    private UUID sagaId(UUID orderId) {
        return orderDb.queryForObject("SELECT saga_id FROM saga_instances WHERE order_id = ?", UUID.class, orderId);
    }

    /** finding_type → detected_at for the order. */
    private Map<String, Instant> detections(UUID orderId) {
        Map<String, Instant> found = new TreeMap<>();
        reconciliationDb.query("SELECT finding_type, detected_at FROM findings WHERE order_id = ?",
                rs -> {
                    found.put(rs.getString(1), rs.getTimestamp(2).toInstant());
                }, orderId);
        return found;
    }

    private boolean resolved(UUID orderId, Set<String> types) {
        return reconciliationDb.queryForObject(
                "SELECT count(*) FROM findings WHERE order_id = ? AND finding_type = ANY (?) AND resolved_at IS NOT NULL",
                Long.class, orderId, types.toArray(new String[0])) == types.size();
    }

    private static Map<String, Long> countBy(List<String> values) {
        Map<String, Long> counts = new TreeMap<>();
        values.forEach(v -> counts.merge(v, 1L, Long::sum));
        return counts;
    }

    private static boolean waitUntil(Duration timeout, BooleanSupplier condition) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }

    private void writeResults(Instant started, Instant finished, int orders, int perType, Map<String, Integer> mix,
                              Map<String, Long> loadStates, boolean loadTerminal, boolean loadChecked,
                              Map<String, Long> falsePositives, ObjectNode byKind, ArrayNode raw, int injected,
                              long missed, long unexpectedTypes, long repairable, long repaired,
                              boolean allDetected, boolean allRepaired) throws Exception {
        ObjectNode result = DoneCheckResults.header(JSON, "s6-reconciliation",
                "S6: fault-free load must raise no findings; every injected inconsistency must be detected and, "
                        + "where repairable, fixed by Reconciliation's repair command (ADR 0009 §7)", finished);
        ObjectNode params = result.putObject("parameters");
        params.put("loadOrders", orders);
        params.putPOJO("loadMix", mix);
        params.put("loadSeed", SEED);
        params.put("injectionsPerType", perType);
        params.putPOJO("reconciliationTimings", new TreeMap<>(E2eStack.RECONCILIATION_TIMINGS));
        params.put("productionTimings", "check interval PT60S, settle window PT2M, stuck after PT5M, repair retry PT2M");
        params.put("detectDeadlineSeconds", DETECT_DEADLINE.toSeconds());
        params.put("repairDeadlineSeconds", REPAIR_DEADLINE.toSeconds());
        params.put("timeToDetectDefinition", "findings.detected_at (Reconciliation's clock) minus the injection's commit time "
                + "(test JVM clock); includes Debezium relay, settle window and check interval");

        ObjectNode m = result.putObject("measurements");
        m.put("startedAt", started.toString());
        m.put("finishedAt", finished.toString());
        ObjectNode load = m.putObject("faultFreeLoad");
        load.putPOJO("finalStates", loadStates);
        load.put("allTerminal", loadTerminal);
        load.put("allProjectedAndChecked", loadChecked);
        load.putPOJO("findingsByType", falsePositives);
        load.put("falsePositives", falsePositives.values().stream().mapToLong(Long::longValue).sum());
        ObjectNode inj = m.putObject("injections");
        inj.put("injected", injected);
        inj.put("detected", injected - missed);
        inj.put("detectionRate", (double) (injected - missed) / injected);
        inj.put("unexpectedFindingTypes", unexpectedTypes);
        inj.put("repairable", repairable);
        inj.put("repaired", repaired);
        inj.put("autoRepairRate", repairable == 0 ? 0 : (double) repaired / repairable);
        inj.put("allDetectedWithinDeadline", allDetected);
        inj.put("allRepairedWithinDeadline", allRepaired);
        inj.set("byKind", byKind);
        m.set("raw", raw);
        DoneCheckResults.write(JSON, result, finished);
    }
}
