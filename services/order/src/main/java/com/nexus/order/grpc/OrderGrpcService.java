package com.nexus.order.grpc;

import com.nexus.contracts.order.v1.GetOrderRequest;
import com.nexus.contracts.order.v1.LineItem;
import com.nexus.contracts.order.v1.Order;
import com.nexus.contracts.order.v1.OrderServiceGrpc;
import com.nexus.contracts.order.v1.PlaceOrderRequest;
import com.nexus.contracts.order.v1.PlaceOrderResponse;
import com.nexus.order.order.IdempotencyKeyReusedException;
import com.nexus.order.order.InvalidOrderException;
import com.nexus.order.order.OrderService;
import com.nexus.order.order.OrderService.PlaceOrderResult;
import com.nexus.order.order.OrderView;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.UUID;

/**
 * gRPC endpoint for {@code nexus.order.v1.OrderService} (contracts/proto), called by the gateway. Same
 * behaviour as the REST API: both delegate to {@link OrderService}.
 */
@Component
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OrderGrpcService.class);

    private final OrderService orderService;

    public OrderGrpcService(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public void placeOrder(PlaceOrderRequest request, StreamObserver<PlaceOrderResponse> responseObserver) {
        PlaceOrderResult result;
        try {
            result = orderService.placeOrder(request.getIdempotencyKey(), toRequest(request));
        } catch (InvalidOrderException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (IdempotencyKeyReusedException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (RuntimeException e) {
            log.error("PlaceOrder failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription("order could not be placed").asRuntimeException());
            return;
        }
        responseObserver.onNext(PlaceOrderResponse.newBuilder()
                .setOrder(toProto(result.order()))
                .setReplayed(result.replayed())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getOrder(GetOrderRequest request, StreamObserver<Order> responseObserver) {
        UUID orderId;
        try {
            orderId = UUID.fromString(request.getOrderId());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("order_id must be a UUID").asRuntimeException());
            return;
        }
        try {
            orderService.find(orderId).ifPresentOrElse(order -> {
                responseObserver.onNext(toProto(order));
                responseObserver.onCompleted();
            }, () -> responseObserver.onError(Status.NOT_FOUND.withDescription("no order " + orderId).asRuntimeException()));
        } catch (RuntimeException e) {
            log.error("GetOrder failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription("order could not be read").asRuntimeException());
        }
    }

    /**
     * proto3 has no null: an unset string arrives as "" and an unset number as 0, which OrderService's
     * validation rejects with the same messages as for a missing JSON field.
     */
    private static com.nexus.order.order.PlaceOrderRequest toRequest(PlaceOrderRequest r) {
        return new com.nexus.order.order.PlaceOrderRequest(
                r.getCustomerId(),
                r.getItemsList().stream()
                        .map(i -> new com.nexus.order.order.PlaceOrderRequest.Line(i.getSku(), i.getQuantity(), i.getUnitPriceCents()))
                        .toList(),
                r.getCurrency(),
                r.getPaymentMethod());
    }

    private static Order toProto(OrderView view) {
        Order.Builder order = Order.newBuilder()
                .setOrderId(view.orderId().toString())
                .setSagaId(view.sagaId().toString())
                .setStatus(view.status())
                .setStep(Objects.requireNonNullElse(view.step(), ""))
                .setFailureReason(Objects.requireNonNullElse(view.failureReason(), ""))
                .setCustomerId(view.customerId())
                .setTotalCents(view.totalCents())
                .setCurrency(view.currency())
                .setCreatedAt(view.createdAt().toString())
                .setUpdatedAt(view.updatedAt().toString());
        view.items().forEach(i -> order.addItems(LineItem.newBuilder()
                .setSku(i.sku()).setQuantity(i.quantity()).setUnitPriceCents(i.unitPriceCents())));
        return order.build();
    }
}
