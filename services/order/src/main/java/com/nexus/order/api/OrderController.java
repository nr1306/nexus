package com.nexus.order.api;

import com.nexus.order.order.OrderService;
import com.nexus.order.order.OrderService.PlaceOrderResult;
import com.nexus.order.order.OrderView;
import com.nexus.order.order.PlaceOrderRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
class OrderController {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    static final String REPLAYED = "Idempotent-Replayed";

    private final OrderService orderService;

    OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /** Accepts an order. The response is 201 for both the first request and a replay of the same key. */
    @PostMapping
    ResponseEntity<OrderView> placeOrder(@RequestHeader(name = IDEMPOTENCY_KEY, required = false) String idempotencyKey,
                                         @RequestBody(required = false) PlaceOrderRequest request) {
        PlaceOrderResult result = orderService.placeOrder(idempotencyKey, request);
        var location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}").buildAndExpand(result.order().orderId()).toUri();
        return ResponseEntity.created(location)
                .header(REPLAYED, String.valueOf(result.replayed()))
                .body(result.order());
    }

    @GetMapping("/{orderId}")
    ResponseEntity<OrderView> getOrder(@PathVariable UUID orderId) {
        return orderService.find(orderId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }
}
