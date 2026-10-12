package com.nexus.payment.gateway.stripe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.payment.gateway.PaymentGateway.AuthorizationResult;
import com.nexus.payment.gateway.PaymentGateway.OperationResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The Stripe adapter against a stub of Stripe's REST API (ADR 0010); no network, no key needed. */
class StripePaymentGatewayTest {

    private record Request(String method, String path, Map<String, String> headers, Map<String, String> form) {
    }

    private record Reply(int status, String body) {
    }

    private HttpServer server;
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final Map<String, List<Reply>> replies = new LinkedHashMap<>();
    private StripePaymentGateway stripe;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        stripe = gateway("sk_test_123", 5);
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void authorizeCreatesAConfirmedManualCapturePaymentIntent() {
        reply("/v1/payment_intents", 200, "{\"id\":\"pi_1\",\"status\":\"requires_capture\"}");

        AuthorizationResult result = stripe.authorize("order-1:AUTHORIZE", 4999, "USD", "pm_card_visa");

        assertThat(result).isEqualTo(AuthorizationResult.approved("pi_1"));
        Request request = requests.getFirst();
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.headers()).containsEntry("authorization", "Bearer sk_test_123")
                .containsEntry("idempotency-key", "order-1:AUTHORIZE")
                .containsEntry("stripe-version", "2024-06-20");
        assertThat(request.form()).containsEntry("amount", "4999").containsEntry("currency", "usd")
                .containsEntry("payment_method", "pm_card_visa").containsEntry("capture_method", "manual")
                .containsEntry("confirm", "true").containsEntry("payment_method_types[]", "card");
    }

    @Test
    void cardErrorsAreDeclines() {
        reply("/v1/payment_intents", 402, error("card_error", "card_declined", "insufficient_funds"));
        reply("/v1/payment_intents", 402, error("card_error", "card_declined", "generic_decline"));
        reply("/v1/payment_intents", 200, "{\"id\":\"pi_2\",\"status\":\"requires_action\"}");

        assertThat(stripe.authorize("a:AUTHORIZE", 100, "USD", "pm_card_chargeDeclinedInsufficientFunds"))
                .isEqualTo(AuthorizationResult.declined("INSUFFICIENT_FUNDS"));
        assertThat(stripe.authorize("b:AUTHORIZE", 100, "USD", "pm_card_chargeDeclined"))
                .isEqualTo(AuthorizationResult.declined("CARD_DECLINED"));
        assertThat(stripe.authorize("c:AUTHORIZE", 100, "USD", "pm_card_authenticationRequired"))
                .isEqualTo(AuthorizationResult.declined("NOT_AUTHORIZED_REQUIRES_ACTION"));
    }

    @Test
    void captureVoidAndRefundUseThePaymentIntent() {
        reply("/v1/payment_intents/pi_1/capture", 200, "{\"id\":\"pi_1\",\"status\":\"succeeded\"}");
        reply("/v1/payment_intents/pi_2/cancel", 200, "{\"id\":\"pi_2\",\"status\":\"canceled\"}");
        reply("/v1/refunds", 200, "{\"id\":\"re_1\",\"status\":\"succeeded\"}");

        assertThat(stripe.capture("o:CAPTURE", "pi_1", 100, "USD")).isEqualTo(OperationResult.success());
        assertThat(stripe.voidAuthorization("o:VOID", "pi_2")).isEqualTo(OperationResult.success());
        assertThat(stripe.refund("o:REFUND", "pi_1", 100, "USD")).isEqualTo(OperationResult.success());

        assertThat(requests).extracting(Request::path)
                .containsExactly("/v1/payment_intents/pi_1/capture", "/v1/payment_intents/pi_2/cancel", "/v1/refunds");
        assertThat(requests.getLast().form()).containsEntry("payment_intent", "pi_1");
        assertThat(requests).extracting(r -> r.headers().get("idempotency-key")).containsExactly("o:CAPTURE", "o:VOID", "o:REFUND");
    }

    @Test
    void alreadyCanceledOrRefundedIsSuccess() {
        reply("/v1/payment_intents/pi_1/cancel", 400, "{\"error\":{\"type\":\"invalid_request_error\","
                + "\"code\":\"payment_intent_unexpected_state\",\"payment_intent\":{\"id\":\"pi_1\",\"status\":\"canceled\"}}}");
        reply("/v1/refunds", 400, error("invalid_request_error", "charge_already_refunded", null));

        assertThat(stripe.voidAuthorization("o:VOID", "pi_1")).isEqualTo(OperationResult.success());
        assertThat(stripe.refund("o:REFUND", "pi_1", 100, "USD")).isEqualTo(OperationResult.success());
    }

    @Test
    void refusalsAreFailedOutcomes() {
        reply("/v1/payment_intents/pi_1/capture", 402, error("card_error", "card_declined", null));
        reply("/v1/payment_intents/pi_2/cancel", 400, "{\"error\":{\"type\":\"invalid_request_error\","
                + "\"code\":\"payment_intent_unexpected_state\",\"payment_intent\":{\"id\":\"pi_2\",\"status\":\"succeeded\"}}}");
        reply("/v1/refunds", 200, "{\"id\":\"re_1\",\"status\":\"failed\"}");

        assertThat(stripe.capture("o:CAPTURE", "pi_1", 100, "USD")).isEqualTo(OperationResult.failed("CAPTURE_DECLINED"));
        assertThat(stripe.voidAuthorization("o:VOID", "pi_2"))
                .isEqualTo(OperationResult.failed("PAYMENT_INTENT_UNEXPECTED_STATE"));
        assertThat(stripe.refund("o:REFUND", "pi_1", 100, "USD")).isEqualTo(OperationResult.failed("REFUND_DECLINED"));
    }

    @Test
    void serverErrorsAndRateLimitsAreTransient() {
        reply("/v1/payment_intents", 500, "{}");
        reply("/v1/payment_intents", 429, error("rate_limit_error", "rate_limit", null));

        assertThatThrownBy(() -> stripe.authorize("o:AUTHORIZE", 100, "USD", "pm_card_visa"))
                .isInstanceOf(StripeUnavailableException.class).hasMessageContaining("500");
        assertThatThrownBy(() -> stripe.authorize("o:AUTHORIZE", 100, "USD", "pm_card_visa"))
                .isInstanceOf(StripeUnavailableException.class).hasMessageContaining("429");
    }

    @Test
    void timeoutIsTransient() {
        server.removeContext("/");
        server.createContext("/", exchange -> {
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        StripePaymentGateway slow = gateway("sk_test_123", 5, Duration.ofMillis(200));

        assertThatThrownBy(() -> slow.capture("o:CAPTURE", "pi_1", 100, "USD")).isInstanceOf(StripeUnavailableException.class);
    }

    @Test
    void badKeyAndIdempotencyConflictAreNotSwallowed() {
        reply("/v1/payment_intents", 401, error("invalid_request_error", null, null));
        reply("/v1/payment_intents", 400, error("idempotency_error", null, null));

        assertThatThrownBy(() -> stripe.authorize("o:AUTHORIZE", 100, "USD", "pm_card_visa"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("API key");
        assertThatThrownBy(() -> stripe.authorize("o:AUTHORIZE", 100, "USD", "pm_card_visa"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("idempotency");
    }

    @Test
    void circuitOpensAfterRepeatedOutagesAndFailsFast() {
        StripePaymentGateway breaker = gateway("sk_test_123", 2);
        for (int i = 0; i < 2; i++) {
            reply("/v1/payment_intents/pi_1/capture", 503, "{}");
            assertThatThrownBy(() -> breaker.capture("o:CAPTURE", "pi_1", 100, "USD")).isInstanceOf(StripeUnavailableException.class);
        }
        int callsBefore = requests.size();

        assertThatThrownBy(() -> breaker.capture("o:CAPTURE", "pi_1", 100, "USD"))
                .isInstanceOf(StripeUnavailableException.class).hasMessageContaining("circuit breaker is open");
        assertThat(requests).hasSize(callsBefore);
    }

    @Test
    void declinesDoNotOpenTheCircuit() {
        StripePaymentGateway breaker = gateway("sk_test_123", 2);
        for (int i = 0; i < 4; i++) {
            reply("/v1/payment_intents", 402, error("card_error", "card_declined", "generic_decline"));
            assertThat(breaker.authorize("o" + i + ":AUTHORIZE", 100, "USD", "pm_card_chargeDeclined").approved()).isFalse();
        }
        assertThat(requests).hasSize(4);
    }

    @Test
    void onlyTestModeKeysAreAccepted() {
        for (String key : Arrays.asList(null, "", "sk_live_abc", "pk_test_abc", "rk_live_abc")) {
            assertThatThrownBy(() -> gateway(key, 5)).isInstanceOf(IllegalStateException.class).hasMessageContaining("test-mode");
        }
        assertThat(gateway("rk_test_abc", 5).name()).isEqualTo("stripe");
    }

    // --- stub server ---

    private StripePaymentGateway gateway(String key, int windowSize) {
        return gateway(key, windowSize, Duration.ofSeconds(5));
    }

    private StripePaymentGateway gateway(String key, int windowSize, Duration requestTimeout) {
        return new StripePaymentGateway(new StripeProperties(key, "http://localhost:" + server.getAddress().getPort(),
                "2024-06-20", Duration.ofSeconds(2), requestTimeout, 50, windowSize, windowSize, Duration.ofMinutes(1)),
                new ObjectMapper(), new SimpleMeterRegistry());
    }

    private void reply(String path, int status, String body) {
        synchronized (replies) {
            replies.computeIfAbsent(path, p -> new ArrayList<>()).add(new Reply(status, body));
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> form = body.isEmpty() ? Map.of() : Arrays.stream(body.split("&"))
                .map(kv -> kv.split("=", 2))
                .collect(Collectors.toMap(kv -> decode(kv[0]), kv -> kv.length > 1 ? decode(kv[1]) : "", (a, b) -> b, LinkedHashMap::new));
        Map<String, String> headers = new LinkedHashMap<>();
        exchange.getRequestHeaders().forEach((k, v) -> headers.put(k.toLowerCase(), v.getFirst()));
        String path = exchange.getRequestURI().getPath();
        requests.add(new Request(exchange.getRequestMethod(), path, headers, form));
        Reply reply;
        synchronized (replies) {
            List<Reply> queued = replies.getOrDefault(path, new ArrayList<>());
            reply = queued.isEmpty() ? new Reply(404, error("invalid_request_error", "resource_missing", null)) : queued.removeFirst();
        }
        byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Request-Id", "req_test");
        exchange.sendResponseHeaders(reply.status(), bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static String error(String type, String code, String declineCode) {
        StringBuilder json = new StringBuilder("{\"error\":{\"type\":\"").append(type).append('"');
        if (code != null) {
            json.append(",\"code\":\"").append(code).append('"');
        }
        if (declineCode != null) {
            json.append(",\"decline_code\":\"").append(declineCode).append('"');
        }
        return json.append("}}").toString();
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
