package com.nexus.e2e;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.DockerClientFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 1 done criterion (SPEC.md §12): N scripted orders with mixed failures end with 0 stock drift
 * and 0 double charges. Every number is computed from the services' databases, not from saga
 * states alone. With {@code -PresultsDir=...} the measurements are written as JSON (CLAUDE.md
 * benchmark honesty rules); this is a correctness check, not a throughput benchmark.
 */
class PhaseOneDoneCheckIT extends E2eSupport {

    private static final int CONCURRENCY = 16;
    private static final long SEED = 42;

    private static final String VISA = "pm_card_visa";
    private static final String DECLINED = "pm_card_chargeDeclined";
    private static final String NO_FUNDS = "pm_card_chargeDeclinedInsufficientFunds";
    private static final String CAPTURE_FAILS = "pm_mock_captureFails";

    private record Planned(String paymentMethod, Map<String, Integer> items, boolean hasScarce) {
    }

    @Test
    void mixedOrdersEndWithZeroStockDriftAndZeroDoubleCharges() throws Exception {
        int orders = Integer.getInteger("nexus.doneCheck.orders", 200);
        int plentiful = orders * 10;
        int scarce = Math.max(10, orders / 20);
        String skuA = newSku(plentiful);
        String skuB = newSku(plentiful);
        String skuScarce = newSku(scarce);
        Map<String, Integer> initialStock = Map.of(skuA, plentiful, skuB, plentiful, skuScarce, scarce);

        List<Planned> plan = plan(orders, skuA, skuB, skuScarce);
        Instant started = Instant.now();
        Map<UUID, Planned> placed = placeAll(plan);

        JdbcTemplate orderDb = E2eStack.db("order");
        JdbcTemplate inventoryDb = E2eStack.db("inventory");
        JdbcTemplate paymentDb = E2eStack.db("payment");
        Set<UUID> ids = placed.keySet();

        await().atMost(Duration.ofMinutes(15)).pollInterval(Duration.ofSeconds(1))
                .until(() -> countWhere(orderDb, "saga_instances", "order_id", ids, "state NOT IN ('COMPLETED','CANCELLED','NEEDS_ATTENTION')") == 0);
        // Commits after COMPLETED are fire-and-forget: wait until no stock is held for our SKUs.
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(1))
                .until(() -> inventoryDb.queryForObject("SELECT coalesce(sum(reserved), 0) FROM stock WHERE sku IN (?, ?, ?)",
                        Integer.class, skuA, skuB, skuScarce) == 0);
        Duration elapsed = Duration.between(started, Instant.now());

        // --- Outcomes, from order_db ---
        Map<UUID, String> state = new HashMap<>();
        Map<UUID, String> reason = new HashMap<>();
        for (var row : orderDb.queryForList("SELECT order_id, state, failure_reason FROM saga_instances WHERE order_id = ANY (?)",
                (Object) ids.toArray(new UUID[0]))) {
            UUID id = (UUID) row.get("order_id");
            state.put(id, (String) row.get("state"));
            reason.put(id, (String) row.get("failure_reason"));
        }
        long completed = state.values().stream().filter("COMPLETED"::equals).count();
        long needsAttention = state.values().stream().filter("NEEDS_ATTENTION"::equals).count();
        Map<String, Long> cancelledByReason = new TreeMap<>(state.entrySet().stream()
                .filter(e -> e.getValue().equals("CANCELLED"))
                .collect(Collectors.groupingBy(e -> reason.get(e.getKey()), Collectors.counting())));
        long unexpected = placed.entrySet().stream().filter(e -> !expected(e.getValue(), state.get(e.getKey()), reason.get(e.getKey()))).count();

        // --- Stock drift, from inventory_db (and completed orders from order_db) ---
        Map<String, Long> soldByCompletedOrders = new HashMap<>();
        placed.forEach((id, p) -> {
            if ("COMPLETED".equals(state.get(id))) {
                p.items().forEach((sku, qty) -> soldByCompletedOrders.merge(sku, (long) qty, Long::sum));
            }
        });
        long stockDrift = 0;
        Map<String, Object> perSku = new LinkedHashMap<>();
        for (var e : initialStock.entrySet()) {
            String sku = e.getKey();
            var row = inventoryDb.queryForMap("SELECT available, reserved FROM stock WHERE sku = ?", sku);
            long available = ((Number) row.get("available")).longValue();
            long reservedNow = ((Number) row.get("reserved")).longValue();
            long committed = inventoryDb.queryForObject(
                    "SELECT coalesce(sum(quantity), 0) FROM reservations WHERE sku = ? AND status = 'COMMITTED'", Long.class, sku);
            long held = inventoryDb.queryForObject(
                    "SELECT coalesce(sum(quantity), 0) FROM reservations WHERE sku = ? AND status = 'HELD'", Long.class, sku);
            long sold = soldByCompletedOrders.getOrDefault(sku, 0L);
            long drift = Math.abs(e.getValue() - (available + reservedNow + committed))
                    + Math.abs(committed - sold) + reservedNow + held + (available < 0 ? 1 : 0);
            stockDrift += drift;
            perSku.put(skuLabel(sku, skuA, skuB), Map.of("initial", e.getValue(), "available", available,
                    "reserved", reservedNow, "committed", committed, "soldByCompletedOrders", sold, "drift", drift));
        }

        // --- Charges, from payment_db ---
        Map<UUID, List<String>> paymentRows = new HashMap<>();
        for (var row : paymentDb.queryForList("SELECT order_id, operation, status FROM payments WHERE order_id = ANY (?)",
                (Object) ids.toArray(new UUID[0]))) {
            paymentRows.computeIfAbsent((UUID) row.get("order_id"), k -> new ArrayList<>())
                    .add(row.get("operation") + ":" + row.get("status"));
        }
        long capturesTotal = 0;
        long doubleCharges = 0;
        long capturesOnNotCompleted = 0;
        long completedWithoutCapture = 0;
        long danglingAuthorizations = 0;
        for (UUID id : ids) {
            List<String> rows = paymentRows.getOrDefault(id, List.of());
            long captures = rows.stream().filter("CAPTURE:SUCCEEDED"::equals).count();
            capturesTotal += captures;
            if (captures > 1) {
                doubleCharges += captures - 1;
            }
            boolean isCompleted = "COMPLETED".equals(state.get(id));
            if (captures > 0 && !isCompleted) {
                capturesOnNotCompleted++;
            }
            if (isCompleted && captures == 0) {
                completedWithoutCapture++;
            }
            if ("CANCELLED".equals(state.get(id)) && rows.contains("AUTHORIZE:SUCCEEDED") && !rows.contains("VOID:SUCCEEDED")) {
                danglingAuthorizations++;
            }
        }

        ObjectNode measurements = JSON.createObjectNode();
        measurements.put("ordersPlaced", placed.size());
        measurements.put("completed", completed);
        measurements.set("cancelledByReason", JSON.valueToTree(cancelledByReason));
        measurements.put("needsAttention", needsAttention);
        measurements.put("unexpectedOutcomes", unexpected);
        measurements.put("stockDrift", stockDrift);
        measurements.set("stockBySku", JSON.valueToTree(perSku));
        measurements.put("capturesTotal", capturesTotal);
        measurements.put("doubleCharges", doubleCharges);
        measurements.put("capturesOnOrdersNotCompleted", capturesOnNotCompleted);
        measurements.put("completedWithoutCapture", completedWithoutCapture);
        measurements.put("danglingAuthorizations", danglingAuthorizations);
        writeResults(orders, scarce, measurements, elapsed);

        assertThat(placed).hasSize(orders);
        assertThat(needsAttention).as("NEEDS_ATTENTION sagas").isZero();
        assertThat(unexpected).as("orders whose outcome doesn't match their payment method/stock").isZero();
        assertThat(stockDrift).as("stock drift " + perSku).isZero();
        assertThat(doubleCharges).as("double charges").isZero();
        assertThat(capturesOnNotCompleted).as("captures on orders that didn't complete").isZero();
        assertThat(completedWithoutCapture).as("completed orders without a capture").isZero();
        assertThat(danglingAuthorizations).as("cancelled orders still holding an authorization").isZero();
        assertThat(capturesTotal).isEqualTo(completed);
    }

    /** Deterministic mix: 80% visa, 10% declined, 5% insufficient funds, 5% capture fails; 20% include the scarce SKU. */
    private static List<Planned> plan(int orders, String skuA, String skuB, String skuScarce) {
        Random random = new Random(SEED);
        List<Planned> plan = new ArrayList<>();
        for (int i = 0; i < orders; i++) {
            int roll = random.nextInt(100);
            String pm = roll < 80 ? VISA : roll < 90 ? DECLINED : roll < 95 ? NO_FUNDS : CAPTURE_FAILS;
            Map<String, Integer> items = new LinkedHashMap<>();
            items.put(skuA, 1 + random.nextInt(3));
            if (random.nextBoolean()) {
                items.put(skuB, 1 + random.nextInt(2));
            }
            boolean scarce = random.nextInt(100) < 20;
            if (scarce) {
                items.put(skuScarce, 1 + random.nextInt(3));
            }
            plan.add(new Planned(pm, items, scarce));
        }
        return plan;
    }

    private static boolean expected(Planned p, String state, String reason) {
        if (p.hasScarce() && "CANCELLED".equals(state) && "OUT_OF_STOCK".equals(reason)) {
            return true;
        }
        return switch (p.paymentMethod()) {
            case VISA -> "COMPLETED".equals(state);
            case DECLINED -> "CANCELLED".equals(state) && "CARD_DECLINED".equals(reason);
            case NO_FUNDS -> "CANCELLED".equals(state) && "INSUFFICIENT_FUNDS".equals(reason);
            case CAPTURE_FAILS -> "CANCELLED".equals(state) && "CAPTURE_DECLINED".equals(reason);
            default -> false;
        };
    }

    private static Map<UUID, Planned> placeAll(List<Planned> plan) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);
        try {
            List<Future<Map.Entry<UUID, Planned>>> futures = plan.stream()
                    .map(p -> pool.submit(() -> Map.entry(placeOrder(p.paymentMethod(), p.items()), p)))
                    .toList();
            Map<UUID, Planned> placed = new LinkedHashMap<>();
            for (var f : futures) {
                var e = f.get();
                placed.put(e.getKey(), e.getValue());
            }
            return placed;
        } finally {
            pool.shutdownNow();
        }
    }

    private static long countWhere(JdbcTemplate db, String table, String idColumn, Set<UUID> ids, String condition) {
        return db.queryForObject("SELECT count(*) FROM " + table + " WHERE " + idColumn + " = ANY (?) AND " + condition,
                Long.class, (Object) ids.toArray(new UUID[0]));
    }

    private static String skuLabel(String sku, String skuA, String skuB) {
        return sku.equals(skuA) ? "plentifulA" : sku.equals(skuB) ? "plentifulB" : "scarce";
    }

    private static void writeResults(int orders, int scarceStock, ObjectNode measurements, Duration elapsed) throws IOException {
        String dir = System.getProperty("nexus.resultsDir");
        if (dir == null) {
            return;
        }
        Instant now = Instant.now();
        ObjectNode result = JSON.createObjectNode();
        result.put("scenario", "phase1-done-check");
        result.put("description", "SPEC §12 Phase 1 done criterion: mixed-failure orders through the real services "
                + "end with 0 stock drift and 0 double charges. Correctness check, not a throughput benchmark.");
        result.put("timestamp", now.toString());
        Path repo = Path.of(System.getProperty("nexus.repoRoot"));
        result.put("commitSha", git(repo, "rev-parse", "HEAD"));
        result.put("workingTreeDirty", !git(repo, "status", "--porcelain").isBlank());

        ObjectNode env = result.putObject("environment");
        env.put("kind", "local Testcontainers (tests/e2e)");
        env.put("hostOs", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        env.put("hostArch", System.getProperty("os.arch"));
        var docker = DockerClientFactory.instance().client();
        var info = docker.infoCmd().exec();
        env.put("dockerServerVersion", info.getServerVersion());
        env.put("dockerCpus", info.getNCPU());
        env.put("dockerMemoryBytes", info.getMemTotal());
        ObjectNode images = env.putObject("imageArchitectures");
        for (String image : List.of(com.nexus.messaging.testing.NexusContainers.POSTGRES_IMAGE,
                com.nexus.messaging.testing.NexusContainers.KAFKA_IMAGE,
                com.nexus.messaging.testing.NexusContainers.CONNECT_IMAGE, E2eStack.JRE_IMAGE)) {
            images.put(image, docker.inspectImageCmd(image).exec().getArch());
        }
        env.put("kafkaBrokers", 1);
        env.put("topicPartitions", 12);
        env.put("replicasPerService", 1);

        ObjectNode params = result.putObject("parameters");
        params.put("orders", orders);
        params.put("clientConcurrency", CONCURRENCY);
        params.put("seed", SEED);
        params.put("paymentMethodMix", "80% pm_card_visa, 10% pm_card_chargeDeclined, 5% pm_card_chargeDeclinedInsufficientFunds, 5% pm_mock_captureFails");
        params.put("scarceSkuStock", scarceStock);
        params.put("ordersWithScarceSku", "20%");

        result.set("measurements", measurements);
        ObjectNode run = result.putObject("runInfo");
        run.put("elapsedSeconds", elapsed.toMillis() / 1000.0);
        run.put("note", "Wall-clock time from first POST until every saga was terminal and all holds settled, "
                + "on a developer laptop with all components in containers. Not a throughput measurement.");

        Path out = Path.of(dir);
        Files.createDirectories(out);
        String stamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(now);
        Path file = out.resolve("phase1-done-check-" + stamp + ".json");
        Files.writeString(file, JSON.copy().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(result) + "\n",
                StandardCharsets.UTF_8);
        System.out.println("Wrote " + file);
    }

    private static String git(Path repo, String... args) {
        List<String> command = new ArrayList<>(List.of("git", "-C", repo.toString()));
        command.addAll(List.of(args));
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return process.waitFor() == 0 ? output : "unknown";
        } catch (IOException e) {
            return "unknown";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "unknown";
        }
    }
}
