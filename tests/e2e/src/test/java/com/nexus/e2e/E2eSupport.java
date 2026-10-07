package com.nexus.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.awaitility.Awaitility.await;

/** Helpers for driving the system through Order's API and observing the services' databases. */
abstract class E2eSupport {

    static final Set<String> TERMINAL = Set.of("COMPLETED", "CANCELLED", "NEEDS_ATTENTION");

    protected static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    static void startStack() {
        E2eStack.start();
    }

    protected static String newSku(int available) {
        String sku = "E2E-" + UUID.randomUUID();
        E2eStack.db("inventory").update("INSERT INTO stock (sku, available) VALUES (?, ?)", sku, available);
        return sku;
    }

    /** POSTs an order and returns the response body (orderId, status, ...). */
    protected static JsonNode placeOrder(String idempotencyKey, String paymentMethod, Map<String, Integer> items) {
        ObjectNode body = JSON.createObjectNode()
                .put("customerId", "e2e-customer")
                .put("currency", "USD")
                .put("paymentMethod", paymentMethod);
        var lines = body.putArray("items");
        items.forEach((sku, qty) -> lines.addObject().put("sku", sku).put("quantity", qty).put("unitPriceCents", 1_000));
        try {
            HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(E2eStack.orderApi() + "/orders"))
                            .header("Content-Type", "application/json")
                            .header("Idempotency-Key", idempotencyKey)
                            .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 201) {
                throw new IllegalStateException("POST /orders returned " + response.statusCode() + ": " + response.body());
            }
            return JSON.readTree(response.body());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    protected static UUID placeOrder(String paymentMethod, Map<String, Integer> items) {
        return UUID.fromString(placeOrder(UUID.randomUUID().toString(), paymentMethod, items).get("orderId").asText());
    }

    protected static String sagaState(UUID orderId) {
        return E2eStack.db("order").queryForObject("SELECT state FROM saga_instances WHERE order_id = ?", String.class, orderId);
    }

    protected static String failureReason(UUID orderId) {
        return E2eStack.db("order").queryForObject("SELECT failure_reason FROM saga_instances WHERE order_id = ?",
                String.class, orderId);
    }

    protected static String awaitTerminal(UUID orderId) {
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofMillis(250))
                .until(() -> TERMINAL.contains(sagaState(orderId)));
        return sagaState(orderId);
    }

    protected static int available(String sku) {
        return E2eStack.db("inventory").queryForObject("SELECT available FROM stock WHERE sku = ?", Integer.class, sku);
    }

    protected static int reserved(String sku) {
        return E2eStack.db("inventory").queryForObject("SELECT reserved FROM stock WHERE sku = ?", Integer.class, sku);
    }

    /** Payment outcomes for the order as "OPERATION:STATUS", e.g. "CAPTURE:SUCCEEDED". */
    protected static Set<String> payments(UUID orderId) {
        return Set.copyOf(E2eStack.db("payment").queryForList(
                "SELECT operation || ':' || status FROM payments WHERE order_id = ?", String.class, orderId));
    }
}
