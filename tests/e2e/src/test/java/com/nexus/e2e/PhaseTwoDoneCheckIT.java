package com.nexus.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 done criterion (SPEC.md §12): killing any one service mid-load loses no orders once it
 * restarts. Orders are placed at a steady rate while each service in turn is SIGKILLed, kept down,
 * and restarted. A client retries a failed POST with the same Idempotency-Key, as a real client would.
 *
 * <p>An order counts as <b>lost</b> if its POST was never accepted, it never reached a terminal
 * state, it ended {@code NEEDS_ATTENTION}, or it broke a stock, money or fulfillment invariant. A
 * clean {@code CANCELLED} with a {@code TIMEOUT_*} reason (the order was in flight at the killed
 * service for longer than the step deadlines) is not lost; those are reported separately. With
 * {@code -PresultsDir=...} the measurements are written as JSON. Correctness check, not a benchmark.
 */
class PhaseTwoDoneCheckIT extends E2eSupport {

    /** Every application service; Kafka and Connect faults belong to S4 (SPEC §10). */
    private static final List<String> KILL_SEQUENCE = List.of("inventory", "payment", "fraud", "fulfillment", "order");
    private static final List<String> KAFKA_SERVICES = List.of("inventory", "payment", "fulfillment", "order");
    private static final String RESTRICTED_SKU = "SKU-HAZMAT-01";
    private static final long SEED = 42;
    private static final int CLIENT_THREADS = 64;
    private static final Duration WARMUP = Duration.ofSeconds(20);
    private static final Duration GIVE_UP_PLACING = Duration.ofMinutes(5);

    private static final String VISA = "pm_card_visa";
    private static final String DECLINED = "pm_card_chargeDeclined";
    private static final String NO_FUNDS = "pm_card_chargeDeclinedInsufficientFunds";
    private static final String CAPTURE_FAILS = "pm_mock_captureFails";

    private record Planned(String key, String customer, String paymentMethod, Map<String, Integer> items,
                           boolean restricted) {
    }

    /** orderId is null if the POST was never accepted. */
    private record Placed(Planned plan, UUID orderId, int attempts) {
    }

    private record Kill(String service, Instant killedAt, Instant restartedAt, Instant healthyAt) {
    }

    @Test
    void killingAnyOneServiceMidLoadLosesNoOrders() throws Exception {
        double rate = Double.parseDouble(System.getProperty("nexus.doneCheck.rate", "5"));
        Duration downtime = Duration.ofSeconds(Long.getLong("nexus.doneCheck.downtimeSeconds", 10));
        Duration settle = Duration.ofSeconds(Long.getLong("nexus.doneCheck.settleSeconds", 20));
        String run = "p2-" + UUID.randomUUID().toString().substring(0, 8);

        JdbcTemplate orderDb = E2eStack.db("order");
        JdbcTemplate inventoryDb = E2eStack.db("inventory");
        JdbcTemplate paymentDb = E2eStack.db("payment");
        JdbcTemplate fulfillmentDb = E2eStack.db("fulfillment");

        String skuA = newSku(1_000_000);
        String skuB = newSku(1_000_000);
        Map<String, Long> initialStock = new LinkedHashMap<>();
        for (String sku : List.of(skuA, skuB, RESTRICTED_SKU)) {
            initialStock.put(sku, inventoryDb.queryForObject("SELECT available + reserved FROM stock WHERE sku = ?", Long.class, sku)
                    + committed(inventoryDb, sku));
        }
        long restrictedCommittedBefore = committed(inventoryDb, RESTRICTED_SKU);

        // --- Load + kills ---
        Random random = new Random(SEED);
        ExecutorService clients = Executors.newFixedThreadPool(CLIENT_THREADS);
        List<Future<Placed>> futures = new ArrayList<>();
        List<Kill> kills = new ArrayList<>();
        Thread generator = Thread.ofPlatform().name("load-generator").start(() -> {
            long intervalNanos = (long) (1_000_000_000L / rate);
            long next = System.nanoTime();
            int i = 0;
            while (!Thread.currentThread().isInterrupted()) {
                Planned p = plan(random, run, i++, skuA, skuB);
                synchronized (futures) {
                    futures.add(clients.submit(() -> placeUntilAccepted(p)));
                }
                next += intervalNanos;
                long sleep = next - System.nanoTime();
                if (sleep > 0) {
                    try {
                        Thread.sleep(Duration.ofNanos(sleep));
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });

        Instant started = Instant.now();
        try {
            Thread.sleep(WARMUP);
            for (String service : KILL_SEQUENCE) {
                Instant killedAt = Instant.now();
                E2eStack.killService(service);
                Thread.sleep(downtime);
                Instant restartedAt = Instant.now();
                E2eStack.startService(service);
                kills.add(new Kill(service, killedAt, restartedAt, Instant.now()));
                Thread.sleep(settle);
            }
        } finally {
            generator.interrupt();
            generator.join();
            KILL_SEQUENCE.forEach(E2eStack::ensureRunning);
        }
        Instant loadStopped = Instant.now();

        List<Placed> placed = new ArrayList<>();
        synchronized (futures) {
            for (var f : futures) {
                placed.add(f.get());
            }
        }
        clients.shutdownNow();
        Map<UUID, Planned> accepted = new LinkedHashMap<>();
        placed.stream().filter(p -> p.orderId() != null).forEach(p -> accepted.put(p.orderId(), p.plan()));
        long neverAccepted = placed.size() - accepted.size();
        UUID[] ids = accepted.keySet().toArray(new UUID[0]);

        // --- Wait for everything to settle (soft waits: measurements are written even if a wait fails) ---
        boolean allTerminal = waitUntil(Duration.ofMinutes(15), () -> orderDb.queryForObject(
                "SELECT count(*) FROM saga_instances WHERE order_id = ANY (?) AND state NOT IN ('COMPLETED','CANCELLED','NEEDS_ATTENTION')",
                Long.class, (Object) ids) == 0);
        boolean holdsSettled = waitUntil(Duration.ofMinutes(2), () -> inventoryDb.queryForObject(
                "SELECT coalesce(sum(reserved), 0) FROM stock WHERE sku IN (?, ?, ?)", Long.class, skuA, skuB, RESTRICTED_SKU) == 0);
        // Dispatch and notifications follow order.events asynchronously.
        boolean notificationsSettled = waitUntil(Duration.ofMinutes(2), () -> {
            long expected = orderDb.queryForObject(
                    "SELECT coalesce(sum(CASE state WHEN 'COMPLETED' THEN 2 WHEN 'CANCELLED' THEN 1 ELSE 0 END), 0) "
                            + "FROM saga_instances WHERE order_id = ANY (?)", Long.class, (Object) ids);
            return fulfillmentDb.queryForObject("SELECT count(*) FROM notifications WHERE order_id = ANY (?)",
                    Long.class, (Object) ids) >= expected;
        });
        // Cross-check (ADR 0009): Reconciliation has seen every order quiet past its settle window and
        // checked it twice more. Measured before stock and money, so any repair it sent is included.
        JdbcTemplate reconciliationDb = E2eStack.db("reconciliation");
        Duration settleWindow = Duration.parse(E2eStack.RECONCILIATION_TIMINGS.get("RECONCILIATION_SETTLE_WINDOW"));
        Duration checkInterval = Duration.parse(E2eStack.RECONCILIATION_TIMINGS.get("RECONCILIATION_CHECK_INTERVAL"));
        boolean reconciled = waitUntil(Duration.ofMinutes(3), () -> reconciliationDb.queryForObject(
                "SELECT count(*) FROM order_projection WHERE order_id = ANY (?) AND last_event_at < now() - ?::interval",
                Long.class, (Object) ids, settleWindow.plus(checkInterval.multipliedBy(2)).toSeconds() + " seconds") == ids.length);
        Map<String, Long> findings = new TreeMap<>();
        reconciliationDb.queryForList("SELECT finding_type FROM findings WHERE order_id = ANY (?)", String.class, (Object) ids)
                .forEach(type -> findings.merge(type, 1L, Long::sum));
        long findingsTotal = findings.values().stream().mapToLong(Long::longValue).sum();
        Instant settled = Instant.now();

        // --- Outcomes, from order_db ---
        Map<UUID, String> state = new HashMap<>();
        Map<UUID, String> reason = new HashMap<>();
        for (var row : orderDb.queryForList("SELECT order_id, state, failure_reason FROM saga_instances WHERE order_id = ANY (?)",
                (Object) ids)) {
            state.put((UUID) row.get("order_id"), (String) row.get("state"));
            reason.put((UUID) row.get("order_id"), (String) row.get("failure_reason"));
        }
        long completed = count(state, "COMPLETED");
        long needsAttention = count(state, "NEEDS_ATTENTION");
        long notTerminal = accepted.keySet().stream().filter(id -> !TERMINAL.contains(state.get(id))).count();
        Map<String, Long> cancelledByReason = new TreeMap<>(state.entrySet().stream()
                .filter(e -> e.getValue().equals("CANCELLED"))
                .collect(Collectors.groupingBy(e -> String.valueOf(reason.get(e.getKey())), Collectors.counting())));
        long timedOut = accepted.keySet().stream()
                .filter(id -> "CANCELLED".equals(state.get(id)) && reason.getOrDefault(id, "").startsWith("TIMEOUT_")).count();
        long unexpected = accepted.entrySet().stream()
                .filter(e -> !expected(e.getValue(), state.get(e.getKey()), reason.get(e.getKey()))).count();
        // Each planned order has its own customer: more than one order for a customer means a retried POST
        // created a second order.
        long duplicateOrders = orderDb.queryForObject(
                "SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM orders WHERE customer_id LIKE ? GROUP BY customer_id) c",
                Long.class, run + "-%");
        long ordersInDb = orderDb.queryForObject("SELECT count(*) FROM orders WHERE customer_id LIKE ?", Long.class, run + "-%");

        // --- Stock, from inventory_db ---
        Map<String, Long> soldByCompleted = new HashMap<>();
        accepted.forEach((id, p) -> {
            if ("COMPLETED".equals(state.get(id))) {
                p.items().forEach((sku, qty) -> soldByCompleted.merge(sku, (long) qty, Long::sum));
            }
        });
        long stockDrift = 0;
        Map<String, Object> perSku = new LinkedHashMap<>();
        for (var e : initialStock.entrySet()) {
            String sku = e.getKey();
            var row = inventoryDb.queryForMap("SELECT available, reserved FROM stock WHERE sku = ?", sku);
            long available = ((Number) row.get("available")).longValue();
            long reservedNow = ((Number) row.get("reserved")).longValue();
            long committed = committed(inventoryDb, sku);
            long committedByRun = sku.equals(RESTRICTED_SKU) ? committed - restrictedCommittedBefore : committed;
            long held = inventoryDb.queryForObject(
                    "SELECT coalesce(sum(quantity), 0) FROM reservations WHERE sku = ? AND status = 'HELD'", Long.class, sku);
            long sold = soldByCompleted.getOrDefault(sku, 0L);
            long drift = Math.abs(e.getValue() - (available + reservedNow + committed))
                    + Math.abs(committedByRun - sold) + reservedNow + held + (available < 0 ? 1 : 0);
            stockDrift += drift;
            perSku.put(sku.equals(skuA) ? "plentifulA" : sku.equals(skuB) ? "plentifulB" : "restricted",
                    Map.of("initial", e.getValue(), "available", available, "reserved", reservedNow,
                            "committedByRun", committedByRun, "soldByCompletedOrders", sold, "drift", drift));
        }

        // --- Money, from payment_db ---
        Map<UUID, Set<String>> pay = new HashMap<>();
        Map<UUID, Long> captureRows = new HashMap<>();
        for (var row : paymentDb.queryForList("SELECT order_id, operation, status FROM payments WHERE order_id = ANY (?)",
                (Object) ids)) {
            UUID id = (UUID) row.get("order_id");
            String op = row.get("operation") + ":" + row.get("status");
            pay.computeIfAbsent(id, k -> new java.util.HashSet<>()).add(op);
            if (op.equals("CAPTURE:SUCCEEDED")) {
                captureRows.merge(id, 1L, Long::sum);
            }
        }
        long doubleCharges = captureRows.values().stream().mapToLong(n -> n - 1).sum();
        long completedNotCharged = 0;   // COMPLETED but no capture, or captured and refunded
        long cancelledStillCharged = 0; // CANCELLED, captured and not refunded
        long danglingAuthorizations = 0; // CANCELLED, authorized, neither voided nor refunded
        for (UUID id : ids) {
            Set<String> rows = pay.getOrDefault(id, Set.of());
            boolean captured = rows.contains("CAPTURE:SUCCEEDED");
            boolean refunded = rows.contains("REFUND:SUCCEEDED");
            if ("COMPLETED".equals(state.get(id)) && (!captured || refunded)) {
                completedNotCharged++;
            }
            if ("CANCELLED".equals(state.get(id))) {
                if (captured && !refunded) {
                    cancelledStillCharged++;
                }
                if (rows.contains("AUTHORIZE:SUCCEEDED") && !rows.contains("VOID:SUCCEEDED") && !refunded) {
                    danglingAuthorizations++;
                }
            }
        }

        // --- Fulfillment, from fulfillment_db ---
        Map<UUID, String> shipment = new HashMap<>();
        for (var row : fulfillmentDb.queryForList("SELECT order_id, status FROM shipments WHERE order_id = ANY (?)", (Object) ids)) {
            shipment.put((UUID) row.get("order_id"), (String) row.get("status"));
        }
        Map<UUID, Set<String>> notified = new HashMap<>();
        for (var row : fulfillmentDb.queryForList("SELECT order_id, type FROM notifications WHERE order_id = ANY (?)", (Object) ids)) {
            notified.computeIfAbsent((UUID) row.get("order_id"), k -> new java.util.HashSet<>()).add((String) row.get("type"));
        }
        long completedNotShipped = 0;
        long cancelledButShipped = 0;
        long wrongNotifications = 0;
        for (UUID id : ids) {
            Set<String> n = notified.getOrDefault(id, Set.of());
            if ("COMPLETED".equals(state.get(id))) {
                completedNotShipped += "SHIPPED".equals(shipment.get(id)) ? 0 : 1;
                wrongNotifications += n.equals(Set.of("ORDER_CONFIRMED", "ORDER_SHIPPED")) ? 0 : 1;
            } else if ("CANCELLED".equals(state.get(id))) {
                String s = shipment.get(id);
                cancelledButShipped += s == null || s.equals("CANCELLED") || s.equals("REJECTED") ? 0 : 1;
                wrongNotifications += n.equals(Set.of("ORDER_CANCELLED")) ? 0 : 1;
            }
        }

        // --- Dead letters, from each service's DLQ admin ---
        Map<String, Integer> deadLetters = new TreeMap<>();
        for (String service : KAFKA_SERVICES) {
            for (JsonNode topic : getJson(service, "/admin/dlq")) {
                String dlq = topic.asText();
                deadLetters.put(dlq, getJson(service, "/admin/dlq/" + URLEncoder.encode(dlq, StandardCharsets.UTF_8)
                        + "/messages?limit=10000").size());
            }
        }
        int deadLetterTotal = deadLetters.values().stream().mapToInt(Integer::intValue).sum();

        long lost = neverAccepted + notTerminal + needsAttention;

        ObjectNode m = JSON.createObjectNode();
        m.put("ordersPlanned", placed.size());
        m.put("ordersAccepted", accepted.size());
        m.put("postsNeverAccepted", neverAccepted);
        m.put("postsRetried", placed.stream().filter(p -> p.attempts() > 1).count());
        m.put("ordersInOrderDb", ordersInDb);
        m.put("duplicateOrders", duplicateOrders);
        m.put("ordersLost", lost);
        m.put("notTerminal", notTerminal);
        m.put("completed", completed);
        m.set("cancelledByReason", JSON.valueToTree(cancelledByReason));
        m.put("cancelledByTimeout", timedOut);
        m.put("needsAttention", needsAttention);
        m.put("unexpectedOutcomes", unexpected);
        m.put("stockDrift", stockDrift);
        m.set("stockBySku", JSON.valueToTree(perSku));
        m.put("capturesTotal", captureRows.values().stream().mapToLong(Long::longValue).sum());
        m.put("doubleCharges", doubleCharges);
        m.put("completedWithoutNetCharge", completedNotCharged);
        m.put("cancelledStillCharged", cancelledStillCharged);
        m.put("danglingAuthorizations", danglingAuthorizations);
        m.put("completedNotShipped", completedNotShipped);
        m.put("cancelledWithLiveShipment", cancelledButShipped);
        m.put("ordersWithWrongNotifications", wrongNotifications);
        m.set("deadLettersByTopic", JSON.valueToTree(deadLetters));
        m.put("deadLettersTotal", deadLetterTotal);
        m.put("allTerminalBeforeDeadline", allTerminal);
        m.put("holdsSettledBeforeDeadline", holdsSettled);
        m.put("notificationsSettledBeforeDeadline", notificationsSettled);
        m.put("reconciledBeforeDeadline", reconciled);
        m.set("reconciliationFindingsByType", JSON.valueToTree(findings));
        m.put("reconciliationFindings", findingsTotal);
        ArrayNode killLog = m.putArray("kills");
        for (Kill k : kills) {
            killLog.addObject()
                    .put("service", k.service())
                    .put("killedAt", k.killedAt().toString())
                    .put("restartedAt", k.restartedAt().toString())
                    .put("healthyAt", k.healthyAt().toString())
                    .put("downSeconds", Duration.between(k.killedAt(), k.healthyAt()).toMillis() / 1000.0);
        }
        writeResults(rate, downtime, settle, m, started, loadStopped, settled);

        assertThat(lost).as("orders lost (never accepted + not terminal + NEEDS_ATTENTION)").isZero();
        assertThat(unexpected).as("orders whose outcome matches neither their plan nor a clean timeout").isZero();
        assertThat(duplicateOrders).as("orders created twice by a retried POST").isZero();
        assertThat(ordersInDb).as("orders in order_db vs accepted POSTs").isEqualTo(accepted.size());
        assertThat(stockDrift).as("stock drift " + perSku).isZero();
        assertThat(doubleCharges).as("double charges").isZero();
        assertThat(completedNotCharged).as("completed orders without a net charge").isZero();
        assertThat(cancelledStillCharged).as("cancelled orders captured and not refunded").isZero();
        assertThat(danglingAuthorizations).as("cancelled orders still holding an authorization").isZero();
        assertThat(completedNotShipped).as("completed orders not shipped").isZero();
        assertThat(cancelledButShipped).as("cancelled orders with a live shipment").isZero();
        assertThat(wrongNotifications).as("orders without exactly their expected notifications").isZero();
        assertThat(deadLetterTotal).as("dead letters " + deadLetters).isZero();
        assertThat(reconciled).as("every order projected and checked by Reconciliation").isTrue();
        assertThat(findingsTotal).as("Reconciliation findings " + findings).isZero();
        assertThat(kills).hasSize(KILL_SEQUENCE.size());
    }

    /** 75% visa, 8% declined, 4% insufficient funds, 5% capture fails, 8% visa + a restricted SKU. */
    private static Planned plan(Random random, String run, int i, String skuA, String skuB) {
        int roll = random.nextInt(100);
        boolean restricted = roll >= 92;
        String pm = roll < 75 || restricted ? VISA : roll < 83 ? DECLINED : roll < 87 ? NO_FUNDS : CAPTURE_FAILS;
        Map<String, Integer> items = new LinkedHashMap<>();
        items.put(skuA, 1 + random.nextInt(3));
        if (random.nextBoolean()) {
            items.put(skuB, 1 + random.nextInt(2));
        }
        if (restricted) {
            items.put(RESTRICTED_SKU, 1);
        }
        return new Planned(new UUID(SEED, i).toString() + "-" + run, run + "-" + i, pm, items, restricted);
    }

    private static boolean expected(Planned p, String state, String reason) {
        if ("CANCELLED".equals(state) && reason != null && reason.startsWith("TIMEOUT_")) {
            return true;
        }
        if (p.restricted()) {
            return "CANCELLED".equals(state) && "RESTRICTED_SKU".equals(reason);
        }
        return switch (p.paymentMethod()) {
            case VISA -> "COMPLETED".equals(state);
            case DECLINED -> "CANCELLED".equals(state) && "CARD_DECLINED".equals(reason);
            case NO_FUNDS -> "CANCELLED".equals(state) && "INSUFFICIENT_FUNDS".equals(reason);
            case CAPTURE_FAILS -> "CANCELLED".equals(state) && "CAPTURE_DECLINED".equals(reason);
            default -> false;
        };
    }

    /** Retries the POST with the same Idempotency-Key until Order accepts it (Order may be down). */
    private static Placed placeUntilAccepted(Planned p) throws InterruptedException {
        Instant giveUp = Instant.now().plus(GIVE_UP_PLACING);
        int attempts = 0;
        while (Instant.now().isBefore(giveUp)) {
            attempts++;
            try {
                JsonNode body = placeOrder(p.key(), p.customer(), p.paymentMethod(), p.items());
                return new Placed(p, UUID.fromString(body.get("orderId").asText()), attempts);
            } catch (RuntimeException e) {
                Thread.sleep(500);
            }
        }
        return new Placed(p, null, attempts);
    }

    private static JsonNode getJson(String service, String path) throws IOException, InterruptedException {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(E2eStack.serviceApi(service) + path))
                .timeout(Duration.ofSeconds(60)).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException(service + " " + path + " returned " + response.statusCode() + ": " + response.body());
        }
        return JSON.readTree(response.body());
    }

    private static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(1000);
        }
        return condition.getAsBoolean();
    }

    private static long committed(JdbcTemplate inventoryDb, String sku) {
        return inventoryDb.queryForObject(
                "SELECT coalesce(sum(quantity), 0) FROM reservations WHERE sku = ? AND status = 'COMMITTED'", Long.class, sku);
    }

    private static long count(Map<UUID, String> state, String value) {
        return state.values().stream().filter(value::equals).count();
    }

    private static void writeResults(double rate, Duration downtime, Duration settle, ObjectNode measurements,
                                     Instant started, Instant loadStopped, Instant settled) throws IOException {
        Instant now = Instant.now();
        ObjectNode result = DoneCheckResults.header(JSON, "phase2-done-check",
                "SPEC §12 Phase 2 done criterion: each application service in turn is SIGKILLed mid-load and "
                        + "restarted; no order may be lost and every stock, money and fulfillment invariant must hold. "
                        + "Correctness check, not a throughput benchmark.", now);

        ObjectNode params = result.putObject("parameters");
        params.put("targetOrdersPerSecond", rate);
        params.put("clientThreads", CLIENT_THREADS);
        params.put("seed", SEED);
        params.put("killSequence", String.join(",", KILL_SEQUENCE));
        params.put("killSignal", "SIGKILL");
        params.put("warmupSeconds", WARMUP.toSeconds());
        params.put("downtimeBeforeRestartSeconds", downtime.toSeconds());
        params.put("settleAfterRestartSeconds", settle.toSeconds());
        params.put("orderMix", "75% pm_card_visa, 8% pm_card_chargeDeclined, 4% pm_card_chargeDeclinedInsufficientFunds, "
                + "5% pm_mock_captureFails, 8% pm_card_visa + " + RESTRICTED_SKU);
        params.put("clientRetry", "failed POST retried every 0.5 s with the same Idempotency-Key, give up after "
                + GIVE_UP_PLACING.toMinutes() + " min");
        params.put("lostDefinition", "POST never accepted, saga not terminal by the deadline, or NEEDS_ATTENTION");
        params.putPOJO("reconciliationTimings", new TreeMap<>(E2eStack.RECONCILIATION_TIMINGS));
        params.put("timeoutPolicy", "a clean CANCELLED with a TIMEOUT_* reason is allowed and reported as cancelledByTimeout");

        result.set("measurements", measurements);
        ObjectNode runInfo = result.putObject("runInfo");
        runInfo.put("loadSeconds", Duration.between(started, loadStopped).toMillis() / 1000.0);
        runInfo.put("settleSeconds", Duration.between(loadStopped, settled).toMillis() / 1000.0);
        runInfo.put("note", "downSeconds = SIGKILL until the restarted container is healthy. Developer laptop, "
                + "all components in containers, one replica per service. Not a throughput measurement.");
        DoneCheckResults.write(JSON, result, now);
    }
}
