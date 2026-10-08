package com.nexus.order.fraud;

import com.nexus.contracts.fraud.v1.Decision;
import com.nexus.contracts.fraud.v1.EvaluateRequest;
import com.nexus.contracts.fraud.v1.EvaluateResponse;
import com.nexus.contracts.fraud.v1.FraudServiceGrpc;
import com.nexus.order.order.OrderDetails;
import com.nexus.order.order.OrderView;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The real gRPC client against an in-process fake Fraud server. */
class GrpcFraudClientTest {

    enum Behaviour { APPROVE, REJECT, ERROR, SLOW }

    private final AtomicReference<Behaviour> behaviour = new AtomicReference<>(Behaviour.APPROVE);
    private final AtomicInteger serverCalls = new AtomicInteger();
    private final AtomicReference<EvaluateRequest> lastRequest = new AtomicReference<>();
    private Server server;
    private ManagedChannel channel;
    private CircuitBreaker breaker;
    private GrpcFraudClient client;

    @BeforeEach
    void start() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(new FraudServiceGrpc.FraudServiceImplBase() {
            @Override
            public void evaluate(EvaluateRequest request, StreamObserver<EvaluateResponse> response) {
                serverCalls.incrementAndGet();
                lastRequest.set(request);
                switch (behaviour.get()) {
                    case APPROVE -> reply(response, Decision.APPROVE, List.of());
                    case REJECT -> reply(response, Decision.REJECT, List.of("AMOUNT_LIMIT"));
                    case ERROR -> response.onError(Status.UNAVAILABLE.asRuntimeException());
                    case SLOW -> { } // never answers; the client's deadline fires
                }
            }
        }).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        breaker = CircuitBreaker.of("fraud-test", CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(4)
                .minimumNumberOfCalls(4)
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .build());
        client = new GrpcFraudClient(channel, breaker, Duration.ofMillis(200));
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void mapsApproveAndRejectAndSendsOrderFacts() {
        OrderDetails order = order();
        UUID sagaId = UUID.randomUUID();

        assertThat(client.evaluate(order, sagaId).approved()).isTrue();
        EvaluateRequest sent = lastRequest.get();
        assertThat(sent.getOrderId()).isEqualTo(order.orderId().toString());
        assertThat(sent.getSagaId()).isEqualTo(sagaId.toString());
        assertThat(sent.getAmountCents()).isEqualTo(5_000);
        assertThat(sent.getPaymentMethod()).isEqualTo("pm_card_visa");
        assertThat(sent.getItemsList()).hasSize(1);

        behaviour.set(Behaviour.REJECT);
        FraudClient.Verdict rejected = client.evaluate(order, sagaId);
        assertThat(rejected.approved()).isFalse();
        assertThat(rejected.reasons()).containsExactly("AMOUNT_LIMIT");
    }

    @Test
    void deadlineExceededIsUnavailable() {
        behaviour.set(Behaviour.SLOW);

        assertThatThrownBy(() -> client.evaluate(order(), UUID.randomUUID()))
                .isInstanceOf(FraudUnavailableException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED");
    }

    @Test
    void breakerOpensAfterFailuresAndThenFailsFastWithoutCallingFraud() {
        behaviour.set(Behaviour.ERROR);
        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> client.evaluate(order(), UUID.randomUUID())).isInstanceOf(FraudUnavailableException.class);
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        int callsBefore = serverCalls.get();
        behaviour.set(Behaviour.APPROVE);

        assertThatThrownBy(() -> client.evaluate(order(), UUID.randomUUID()))
                .isInstanceOf(FraudUnavailableException.class)
                .hasMessageContaining("circuit breaker open");
        assertThat(serverCalls.get()).isEqualTo(callsBefore);
    }

    private static void reply(StreamObserver<EvaluateResponse> response, Decision decision, List<String> reasons) {
        response.onNext(EvaluateResponse.newBuilder().setDecision(decision).addAllReasons(reasons).build());
        response.onCompleted();
    }

    private static OrderDetails order() {
        return new OrderDetails(UUID.randomUUID(), "customer-1", List.of(new OrderView.Item("SKU-1", 2, 2_500)),
                5_000, "USD", "pm_card_visa");
    }
}
