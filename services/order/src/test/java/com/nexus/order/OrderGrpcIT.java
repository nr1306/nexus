package com.nexus.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.contracts.order.v1.GetOrderRequest;
import com.nexus.contracts.order.v1.LineItem;
import com.nexus.contracts.order.v1.Order;
import com.nexus.contracts.order.v1.OrderServiceGrpc;
import com.nexus.contracts.order.v1.PlaceOrderRequest;
import com.nexus.contracts.order.v1.PlaceOrderResponse;
import com.nexus.order.grpc.GrpcServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The gateway-facing gRPC API over a real channel: same behaviour as the REST API (OrderApiIT). */
class OrderGrpcIT extends OrderIntegrationTest {

    @Autowired
    GrpcServer grpcServer;

    @Autowired
    MockMvc mvc;

    @Autowired
    ObjectMapper objectMapper;

    private ManagedChannel channel;
    private OrderServiceGrpc.OrderServiceBlockingStub orders;

    @BeforeEach
    void connect() {
        channel = ManagedChannelBuilder.forAddress("localhost", grpcServer.port()).usePlaintext().build();
        orders = OrderServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void disconnect() {
        channel.shutdownNow();
    }

    @Test
    void placeOrderStartsSagaAndGetOrderReturnsIt() {
        PlaceOrderResponse placed = orders.placeOrder(request(UUID.randomUUID().toString(), 2));

        Order order = placed.getOrder();
        assertThat(placed.getReplayed()).isFalse();
        assertThat(order.getStatus()).isEqualTo("PENDING");
        assertThat(order.getStep()).isEqualTo("RESERVE_INVENTORY");
        assertThat(order.getFailureReason()).isEmpty();
        assertThat(order.getTotalCents()).isEqualTo(5000);
        assertThat(order.getItemsList()).extracting(LineItem::getSku).containsExactly("SKU-0001", "SKU-0002");
        UUID orderId = UUID.fromString(order.getOrderId());
        assertThat(commands(orderId)).containsExactly("ReserveInventory");
        assertThat(orderEvents(orderId)).containsExactly("OrderCreated");

        assertThat(orders.getOrder(GetOrderRequest.newBuilder().setOrderId(order.getOrderId()).build())).isEqualTo(order);
    }

    @Test
    void replayWithSameKeyReturnsOriginalOrder() {
        String key = UUID.randomUUID().toString();
        Order first = orders.placeOrder(request(key, 2)).getOrder();

        PlaceOrderResponse replay = orders.placeOrder(request(key, 2));

        assertThat(replay.getReplayed()).isTrue();
        assertThat(replay.getOrder().getOrderId()).isEqualTo(first.getOrderId());
        assertThat(commands(UUID.fromString(first.getOrderId()))).containsExactly("ReserveInventory");
    }

    @Test
    void keyIsSharedWithTheRestApi() throws Exception {
        String key = UUID.randomUUID().toString();
        String body = """
                {"customerId":"customer-1","currency":"USD","paymentMethod":"pm_card_visa",
                 "items":[{"sku":"SKU-0001","quantity":2,"unitPriceCents":1500},
                          {"sku":"SKU-0002","quantity":1,"unitPriceCents":2000}]}
                """;
        String restOrderId = objectMapper.readTree(mvc.perform(post("/orders").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("orderId").asText();

        PlaceOrderResponse replay = orders.placeOrder(request(key, 2));

        assertThat(replay.getReplayed()).isTrue();
        assertThat(replay.getOrder().getOrderId()).isEqualTo(restOrderId);
    }

    @Test
    void sameKeyWithDifferentRequestIsFailedPrecondition() {
        String key = UUID.randomUUID().toString();
        orders.placeOrder(request(key, 2));

        assertThatThrownBy(() -> orders.placeOrder(request(key, 3)))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
    }

    @Test
    void invalidRequestsAreInvalidArgument() {
        assertCode(() -> orders.placeOrder(request("", 2)), Status.Code.INVALID_ARGUMENT);
        assertCode(() -> orders.placeOrder(request(UUID.randomUUID().toString(), 2).toBuilder().clearItems().build()),
                Status.Code.INVALID_ARGUMENT);
        assertCode(() -> orders.placeOrder(request(UUID.randomUUID().toString(), 0)), Status.Code.INVALID_ARGUMENT);
        assertCode(() -> orders.placeOrder(request(UUID.randomUUID().toString(), 2).toBuilder().setCurrency("usd").build()),
                Status.Code.INVALID_ARGUMENT);
        assertCode(() -> orders.getOrder(GetOrderRequest.newBuilder().setOrderId("not-a-uuid").build()),
                Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void unknownOrderIsNotFound() {
        assertCode(() -> orders.getOrder(GetOrderRequest.newBuilder().setOrderId(UUID.randomUUID().toString()).build()),
                Status.Code.NOT_FOUND);
    }

    private static PlaceOrderRequest request(String key, int firstQuantity) {
        return PlaceOrderRequest.newBuilder()
                .setIdempotencyKey(key)
                .setCustomerId("customer-1")
                .setCurrency("USD")
                .setPaymentMethod("pm_card_visa")
                .addItems(LineItem.newBuilder().setSku("SKU-0001").setQuantity(firstQuantity).setUnitPriceCents(1500))
                .addItems(LineItem.newBuilder().setSku("SKU-0002").setQuantity(1).setUnitPriceCents(2000))
                .build();
    }

    private static void assertCode(Runnable call, Status.Code code) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(StatusRuntimeException.class,
                e -> assertThat(e.getStatus().getCode()).isEqualTo(code));
    }
}
