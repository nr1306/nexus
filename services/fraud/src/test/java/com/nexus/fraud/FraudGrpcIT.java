package com.nexus.fraud;

import com.nexus.contracts.fraud.v1.Decision;
import com.nexus.contracts.fraud.v1.EvaluateRequest;
import com.nexus.contracts.fraud.v1.EvaluateResponse;
import com.nexus.contracts.fraud.v1.FraudServiceGrpc;
import com.nexus.fraud.grpc.GrpcServer;
import com.nexus.messaging.testing.NexusContainers;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Fraud over real gRPC against real Postgres. */
@SpringBootTest(properties = "nexus.fraud.grpc-port=0")
class FraudGrpcIT {

    private static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(Network.newNetwork(), "fraud_db");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    GrpcServer grpcServer;

    @Autowired
    JdbcTemplate jdbc;

    private ManagedChannel channel;
    private FraudServiceGrpc.FraudServiceBlockingStub fraud;

    @BeforeEach
    void connect() {
        channel = ManagedChannelBuilder.forAddress("localhost", grpcServer.port()).usePlaintext().build();
        fraud = FraudServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void disconnect() {
        channel.shutdownNow();
    }

    @Test
    void ordinaryOrderIsApproved() {
        EvaluateResponse response = fraud.evaluate(request(UUID.randomUUID(), newCustomer(), 4_999, "pm_card_visa"));

        assertThat(response.getDecision()).isEqualTo(Decision.APPROVE);
        assertThat(response.getReasonsList()).isEmpty();
    }

    @Test
    void amountAboveLimitIsRejected() {
        EvaluateResponse response = fraud.evaluate(request(UUID.randomUUID(), newCustomer(), 500_001, "pm_card_visa"));

        assertThat(response.getDecision()).isEqualTo(Decision.REJECT);
        assertThat(response.getReasonsList()).containsExactly("AMOUNT_LIMIT");
    }

    @Test
    void blocklistedPaymentMethodAndCustomerAreRejected() {
        assertThat(fraud.evaluate(request(UUID.randomUUID(), newCustomer(), 100, "pm_card_radarBlock")).getReasonsList())
                .containsExactly("BLOCKLISTED_PAYMENT_METHOD");
        assertThat(fraud.evaluate(request(UUID.randomUUID(), "blocked-customer", 100, "pm_card_visa")).getReasonsList())
                .containsExactly("BLOCKLISTED_CUSTOMER");
    }

    @Test
    void sixthOrderInWindowTripsVelocity() {
        String customer = newCustomer();
        for (int i = 0; i < 5; i++) {
            assertThat(fraud.evaluate(request(UUID.randomUUID(), customer, 100, "pm_card_visa")).getDecision())
                    .isEqualTo(Decision.APPROVE);
        }

        EvaluateResponse sixth = fraud.evaluate(request(UUID.randomUUID(), customer, 100, "pm_card_visa"));

        assertThat(sixth.getDecision()).isEqualTo(Decision.REJECT);
        assertThat(sixth.getReasonsList()).containsExactly("VELOCITY");
    }

    @Test
    void repeatedEvaluationReturnsStoredDecisionEvenIfFactsChanged() {
        String customer = newCustomer();
        UUID first = UUID.randomUUID();
        assertThat(fraud.evaluate(request(first, customer, 100, "pm_card_visa")).getDecision()).isEqualTo(Decision.APPROVE);
        for (int i = 0; i < 5; i++) {
            fraud.evaluate(request(UUID.randomUUID(), customer, 100, "pm_card_visa"));
        }

        // The customer is now over the velocity limit, but the first order's decision stands.
        assertThat(fraud.evaluate(request(first, customer, 100, "pm_card_visa")).getDecision()).isEqualTo(Decision.APPROVE);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM decisions WHERE order_id = ?", Integer.class, first)).isEqualTo(1);
    }

    @Test
    void concurrentEvaluationsOfOneOrderAgree() throws Exception {
        UUID orderId = UUID.randomUUID();
        EvaluateRequest request = request(orderId, newCustomer(), 100, "pm_card_visa");
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Decision>> calls = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            calls.add(() -> {
                start.await();
                return fraud.evaluate(request).getDecision();
            });
        }
        ExecutorService pool = Executors.newFixedThreadPool(8);
        Set<Decision> decisions = new HashSet<>();
        try {
            List<Future<Decision>> futures = calls.stream().map(pool::submit).toList();
            start.countDown();
            for (Future<Decision> f : futures) {
                decisions.add(f.get());
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(decisions).containsExactly(Decision.APPROVE);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM decisions WHERE order_id = ?", Integer.class, orderId)).isEqualTo(1);
    }

    @Test
    void invalidRequestIsInvalidArgument() {
        EvaluateRequest bad = request(UUID.randomUUID(), newCustomer(), 100, "pm_card_visa").toBuilder()
                .setOrderId("not-a-uuid").build();

        assertThatThrownBy(() -> fraud.evaluate(bad))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    private static String newCustomer() {
        return "customer-" + UUID.randomUUID();
    }

    private static EvaluateRequest request(UUID orderId, String customer, long amountCents, String paymentMethod) {
        return EvaluateRequest.newBuilder()
                .setOrderId(orderId.toString())
                .setSagaId(UUID.randomUUID().toString())
                .setCustomerId(customer)
                .setAmountCents(amountCents)
                .setCurrency("USD")
                .setPaymentMethod(paymentMethod)
                .build();
    }
}
