package com.nexus.order.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.order.order.IdempotencyKeyRepository.StoredKey;
import com.nexus.order.saga.SagaOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Accepts orders. Shared by the REST API now and the gRPC endpoint the gateway will call (Phase 3).
 */
@Service
public class OrderService {

    public record PlaceOrderResult(OrderView order, boolean replayed) {
    }

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");
    private static final int MAX_KEY_LENGTH = 255;
    private static final int MAX_ITEMS = 100;

    private final OrderRepository orders;
    private final IdempotencyKeyRepository idempotencyKeys;
    private final SagaOrchestrator orchestrator;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orders, IdempotencyKeyRepository idempotencyKeys,
                        SagaOrchestrator orchestrator, ObjectMapper objectMapper) {
        this.orders = orders;
        this.idempotencyKeys = idempotencyKeys;
        this.orchestrator = orchestrator;
        this.objectMapper = objectMapper;
    }

    /**
     * Creates the order and starts its saga in one transaction. Repeating the same Idempotency-Key
     * with the same body returns the original order; with a different body it is rejected.
     */
    @Transactional
    public PlaceOrderResult placeOrder(String idempotencyKey, PlaceOrderRequest request) {
        validateKey(idempotencyKey);
        List<OrderView.Item> items = validate(request);
        String requestHash = hash(request);

        UUID orderId = UUID.randomUUID();
        if (!idempotencyKeys.claim(idempotencyKey, requestHash, orderId)) {
            StoredKey stored = idempotencyKeys.find(idempotencyKey);
            if (!stored.requestHash().equals(requestHash)) {
                throw new IdempotencyKeyReusedException(idempotencyKey);
            }
            return new PlaceOrderResult(orders.findView(stored.orderId()).orElseThrow(), true);
        }

        OrderDetails order = new OrderDetails(orderId, request.customerId(), items, total(items),
                request.currency(), request.paymentMethod());
        UUID sagaId = UUID.randomUUID();
        try (var o = MDC.putCloseable("orderId", orderId.toString());
             var s = MDC.putCloseable("sagaId", sagaId.toString())) {
            orders.insert(order);
            orchestrator.start(order, sagaId);
            log.info("Order accepted: {} item(s), total {} {}", items.size(), order.totalCents(), order.currency());
        }
        return new PlaceOrderResult(orders.findView(orderId).orElseThrow(), false);
    }

    @Transactional(readOnly = true)
    public Optional<OrderView> find(UUID orderId) {
        return orders.findView(orderId);
    }

    private static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new InvalidOrderException("Idempotency-Key header is required");
        }
        if (key.length() > MAX_KEY_LENGTH) {
            throw new InvalidOrderException("Idempotency-Key must be at most " + MAX_KEY_LENGTH + " characters");
        }
    }

    private static List<OrderView.Item> validate(PlaceOrderRequest request) {
        if (request == null) {
            throw new InvalidOrderException("Request body is required");
        }
        require(request.customerId() != null && !request.customerId().isBlank(), "customerId is required");
        require(request.currency() != null && CURRENCY.matcher(request.currency()).matches(),
                "currency must be an ISO 4217 code such as USD");
        require(request.paymentMethod() != null && !request.paymentMethod().isBlank(), "paymentMethod is required");
        require(request.items() != null && !request.items().isEmpty(), "items must not be empty");
        require(request.items().size() <= MAX_ITEMS, "at most " + MAX_ITEMS + " items per order");

        Set<String> skus = new HashSet<>();
        for (PlaceOrderRequest.Line line : request.items()) {
            require(line != null && line.sku() != null && !line.sku().isBlank(), "every item needs a sku");
            require(skus.add(line.sku()), "duplicate sku " + line.sku() + "; combine the quantities");
            require(line.quantity() != null && line.quantity() > 0, "quantity must be positive for " + line.sku());
            require(line.unitPriceCents() != null && line.unitPriceCents() > 0, "unitPriceCents must be positive for " + line.sku());
        }
        return request.items().stream()
                .map(l -> new OrderView.Item(l.sku(), l.quantity(), l.unitPriceCents()))
                .toList();
    }

    private static long total(List<OrderView.Item> items) {
        try {
            long total = 0;
            for (OrderView.Item item : items) {
                total = Math.addExact(total, Math.multiplyExact(item.unitPriceCents(), (long) item.quantity()));
            }
            return total;
        } catch (ArithmeticException e) {
            throw new InvalidOrderException("order total is too large");
        }
    }

    private String hash(PlaceOrderRequest request) {
        try {
            byte[] canonical = objectMapper.writeValueAsString(request).getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Cannot hash request", e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new InvalidOrderException(message);
        }
    }
}
