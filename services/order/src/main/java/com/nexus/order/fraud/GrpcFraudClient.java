package com.nexus.order.fraud;

import com.nexus.contracts.fraud.v1.EvaluateRequest;
import com.nexus.contracts.fraud.v1.EvaluateResponse;
import com.nexus.contracts.fraud.v1.FraudServiceGrpc;
import com.nexus.contracts.fraud.v1.LineItem;
import com.nexus.order.order.OrderDetails;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Calls Fraud over gRPC with a per-call deadline, behind a Resilience4j circuit breaker so a failing
 * Fraud service is not hammered and callers fail fast while it is down.
 */
public class GrpcFraudClient implements FraudClient {

    private final FraudServiceGrpc.FraudServiceBlockingStub stub;
    private final CircuitBreaker circuitBreaker;
    private final Duration deadline;

    public GrpcFraudClient(Channel channel, CircuitBreaker circuitBreaker, Duration deadline) {
        this.stub = FraudServiceGrpc.newBlockingStub(channel);
        this.circuitBreaker = circuitBreaker;
        this.deadline = deadline;
    }

    @Override
    public Verdict evaluate(OrderDetails order, UUID sagaId) {
        EvaluateRequest request = EvaluateRequest.newBuilder()
                .setOrderId(order.orderId().toString())
                .setSagaId(sagaId.toString())
                .setCustomerId(order.customerId())
                .setAmountCents(order.totalCents())
                .setCurrency(order.currency())
                .setPaymentMethod(order.paymentMethod())
                .addAllItems(order.items().stream()
                        .map(i -> LineItem.newBuilder().setSku(i.sku()).setQuantity(i.quantity()).build())
                        .toList())
                .build();
        EvaluateResponse response;
        try {
            response = circuitBreaker.executeSupplier(
                    () -> stub.withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS).evaluate(request));
        } catch (CallNotPermittedException e) {
            throw new FraudUnavailableException("circuit breaker open", e);
        } catch (StatusRuntimeException e) {
            throw new FraudUnavailableException("fraud call failed: " + e.getStatus().getCode(), e);
        }
        return switch (response.getDecision()) {
            case APPROVE -> new Verdict(true, response.getReasonsList());
            case REJECT -> new Verdict(false, response.getReasonsList());
            default -> throw new FraudUnavailableException("unexpected decision " + response.getDecision(), null);
        };
    }
}
