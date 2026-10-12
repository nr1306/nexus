package com.nexus.payment.gateway.stripe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.payment.gateway.PaymentGateway;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Stripe <b>test mode</b> adapter (ADR 0010), over Stripe's REST API with the JDK HTTP client.
 *
 * <ul>
 *   <li>Authorize = a confirmed PaymentIntent with {@code capture_method=manual}; the PaymentIntent id is
 *       the authorization id.</li>
 *   <li>Capture / void = {@code /capture} / {@code /cancel} on that PaymentIntent; refund = a Refund for it.</li>
 *   <li>Every request sends our {@code <orderId>:<operation>} key as Stripe's {@code Idempotency-Key}, so a
 *       repeated call returns Stripe's original result (ADR 0002).</li>
 *   <li>Declines and refusals are returned as business outcomes. Timeouts, connection errors, 429 and 5xx
 *       throw {@link StripeUnavailableException} (retried by the retry topics) and count towards the
 *       circuit breaker; while it is open, calls fail fast with the same exception.</li>
 * </ul>
 * It refuses to start with anything but a test-mode key: Nexus never moves real money.
 */
@Component
@ConditionalOnProperty(name = "nexus.payment.gateway", havingValue = "stripe")
public class StripePaymentGateway implements PaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(StripePaymentGateway.class);
    private static final Pattern TEST_KEY = Pattern.compile("^(sk|rk)_test_\\S+$");
    private static final Set<String> REFUND_OK = Set.of("succeeded", "pending", "requires_action");

    private final StripeProperties properties;
    private final ObjectMapper objectMapper;
    private final HttpClient http;
    private final CircuitBreaker circuitBreaker;

    public StripePaymentGateway(StripeProperties properties, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        if (properties.apiKey() == null || !TEST_KEY.matcher(properties.apiKey()).matches()) {
            throw new IllegalStateException("nexus.payment.gateway=stripe needs a Stripe test-mode key "
                    + "(sk_test_... or rk_test_...) in STRIPE_API_KEY; live keys are refused");
        }
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.http = HttpClient.newBuilder().connectTimeout(properties.connectTimeout()).build();
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .failureRateThreshold(properties.failureRateThreshold())
                .slidingWindowSize(properties.slidingWindowSize())
                .minimumNumberOfCalls(properties.minimumCalls())
                .waitDurationInOpenState(properties.openStateWait())
                .recordExceptions(StripeUnavailableException.class)
                .build());
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry);
        this.circuitBreaker = registry.circuitBreaker("stripe");
    }

    @Override
    public String name() {
        return "stripe";
    }

    @Override
    public AuthorizationResult authorize(String idempotencyKey, long amountCents, String currency, String paymentMethod) {
        Map<String, String> form = new LinkedHashMap<>();
        form.put("amount", Long.toString(amountCents));
        form.put("currency", currency.toLowerCase(Locale.ROOT));
        form.put("payment_method", paymentMethod);
        form.put("payment_method_types[]", "card");
        form.put("capture_method", "manual");
        form.put("confirm", "true");
        form.put("metadata[nexus_idempotency_key]", idempotencyKey);
        Response r = post("/v1/payment_intents", idempotencyKey, form);
        if (r.ok()) {
            String status = r.body().path("status").asText();
            return "requires_capture".equals(status)
                    ? AuthorizationResult.approved(r.body().path("id").asText())
                    // e.g. requires_action (3-D Secure): no customer to authenticate in a backend saga.
                    : AuthorizationResult.declined("NOT_AUTHORIZED_" + status.toUpperCase(Locale.ROOT));
        }
        if ("card_error".equals(r.errorType())) {
            return AuthorizationResult.declined("insufficient_funds".equals(r.error().path("decline_code").asText())
                    ? "INSUFFICIENT_FUNDS" : "CARD_DECLINED");
        }
        return AuthorizationResult.declined(r.errorCode("AUTHORIZATION_FAILED"));
    }

    @Override
    public OperationResult capture(String idempotencyKey, String authorizationId, long amountCents, String currency) {
        Response r = post("/v1/payment_intents/" + encode(authorizationId) + "/capture", idempotencyKey, Map.of());
        if (r.ok()) {
            return "succeeded".equals(r.body().path("status").asText())
                    ? OperationResult.success() : OperationResult.failed("CAPTURE_" + r.body().path("status").asText().toUpperCase(Locale.ROOT));
        }
        return OperationResult.failed("card_error".equals(r.errorType()) ? "CAPTURE_DECLINED" : r.errorCode("CAPTURE_FAILED"));
    }

    @Override
    public OperationResult voidAuthorization(String idempotencyKey, String authorizationId) {
        Response r = post("/v1/payment_intents/" + encode(authorizationId) + "/cancel", idempotencyKey, Map.of());
        // Already canceled (e.g. Stripe expired the authorization) is the outcome we want.
        if (r.ok() || "canceled".equals(r.error().path("payment_intent").path("status").asText())) {
            return OperationResult.success();
        }
        return OperationResult.failed(r.errorCode("VOID_FAILED"));
    }

    @Override
    public OperationResult refund(String idempotencyKey, String authorizationId, long amountCents, String currency) {
        Response r = post("/v1/refunds", idempotencyKey, Map.of("payment_intent", authorizationId));
        if (r.ok()) {
            return REFUND_OK.contains(r.body().path("status").asText())
                    ? OperationResult.success() : OperationResult.failed("REFUND_DECLINED");
        }
        if ("charge_already_refunded".equals(r.error().path("code").asText())) {
            return OperationResult.success();
        }
        return OperationResult.failed(r.errorCode("REFUND_DECLINED"));
    }

    /** A completed HTTP exchange that is not transient: 2xx, or a 4xx with Stripe's error object. */
    private record Response(int status, JsonNode body) {

        boolean ok() {
            return status / 100 == 2;
        }

        JsonNode error() {
            return body.path("error");
        }

        String errorType() {
            return error().path("type").asText();
        }

        String errorCode(String fallback) {
            String code = error().path("code").asText();
            return code.isEmpty() ? fallback : code.toUpperCase(Locale.ROOT);
        }
    }

    private Response post(String path, String idempotencyKey, Map<String, String> form) {
        try {
            return circuitBreaker.executeSupplier(() -> send(path, idempotencyKey, form));
        } catch (CallNotPermittedException e) {
            throw new StripeUnavailableException("Stripe circuit breaker is open", e);
        }
    }

    private Response send(String path, String idempotencyKey, Map<String, String> form) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(properties.baseUrl() + path))
                .timeout(properties.requestTimeout())
                .header("Authorization", "Bearer " + properties.apiKey())
                .header("Stripe-Version", properties.apiVersion())
                .header("Idempotency-Key", idempotencyKey)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form.entrySet().stream()
                        .map(e -> encode(e.getKey()) + "=" + encode(e.getValue()))
                        .collect(Collectors.joining("&"))))
                .build();
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new StripeUnavailableException("Stripe call failed: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StripeUnavailableException("Interrupted during Stripe call", e);
        }
        int status = response.statusCode();
        String requestId = response.headers().firstValue("Request-Id").orElse("-");
        if (status == 429 || status >= 500) {
            throw new StripeUnavailableException("Stripe returned " + status + " (request " + requestId + ")");
        }
        if (status == 401 || status == 403) {
            // Not transient, but retrying lets the command reach the DLQ for replay once the key is fixed.
            throw new IllegalStateException("Stripe rejected the API key: " + status + " (request " + requestId + ")");
        }
        JsonNode body;
        try {
            body = objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new StripeUnavailableException("Unreadable Stripe response " + status + " (request " + requestId + ")", e);
        }
        if (status == 409 || status == 400 && "idempotency_error".equals(body.path("error").path("type").asText())) {
            // Our key was reused with different parameters: a bug on our side, never a retry away from success.
            throw new IllegalStateException("Stripe idempotency conflict for " + idempotencyKey + " (request " + requestId + ")");
        }
        if (status / 100 != 2) {
            log.info("Stripe {} {}: {} (request {})", path, status, body.path("error").path("code").asText(), requestId);
        }
        return new Response(status, body);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
