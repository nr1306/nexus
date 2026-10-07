package com.nexus.payment;

import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.payment.gateway.MockPaymentGateway;
import com.nexus.payment.messaging.PaymentCommandHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the command handler (idempotency + payment rules) directly against Postgres.
 */
class PaymentIT extends PaymentIntegrationTest {

    private static final String VISA = "pm_card_visa";

    @Autowired
    PaymentCommandHandler handler;

    @Autowired
    MockPaymentGateway gateway;

    @Test
    void authorizeApprovedRepliesPaymentAuthorized() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope command = authorizeCommand(orderId, 4999, VISA);

        handler.handle(command);

        EventEnvelope reply = single(replies(orderId));
        assertThat(reply.eventType()).isEqualTo("PaymentAuthorized");
        assertThat(reply.sagaId()).isEqualTo(command.sagaId());
        assertThat(reply.payload().get("amountCents").asLong()).isEqualTo(4999);
        assertThat(reply.payload().get("currency").asText()).isEqualTo("USD");
        assertThat(reply.payload().get("authorizationId").asText()).startsWith("mock_auth_");
        assertThat(gateway.calls(orderId + ":AUTHORIZE")).isEqualTo(1);
    }

    @Test
    void declinedCardRepliesPaymentDeclined() {
        UUID declined = UUID.randomUUID();
        UUID noFunds = UUID.randomUUID();

        handler.handle(authorizeCommand(declined, 4999, MockPaymentGateway.DECLINED));
        handler.handle(authorizeCommand(noFunds, 4999, MockPaymentGateway.INSUFFICIENT_FUNDS));

        assertThat(reason(single(replies(declined)))).isEqualTo("CARD_DECLINED");
        assertThat(single(replies(declined)).eventType()).isEqualTo("PaymentDeclined");
        assertThat(reason(single(replies(noFunds)))).isEqualTo("INSUFFICIENT_FUNDS");
    }

    @Test
    void invalidAuthorizeIsDeclinedWithoutCallingProvider() {
        UUID orderId = UUID.randomUUID();
        var payload = objectMapper.createObjectNode().put("amountCents", -5).put("currency", "usd").put("paymentMethod", VISA);

        handler.handle(command("AuthorizePayment", orderId, payload));

        assertThat(reason(single(replies(orderId)))).isEqualTo("INVALID_REQUEST");
        assertThat(gateway.calls(orderId + ":AUTHORIZE")).isZero();
    }

    @Test
    void duplicateDeliveryOfAuthorizeHoldsFundsOnce() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope command = authorizeCommand(orderId, 4999, VISA);

        assertThat(handler.handle(command)).isTrue();
        assertThat(handler.handle(command)).isFalse();

        assertThat(replies(orderId)).hasSize(1);
        assertThat(paymentRows(orderId, "AUTHORIZE")).isEqualTo(1);
        assertThat(gateway.calls(orderId + ":AUTHORIZE")).isEqualTo(1);
    }

    @Test
    void retriedAuthorizeWithNewEventIdRepeatsOutcomeWithoutCallingProvider() {
        UUID orderId = UUID.randomUUID();

        handler.handle(authorizeCommand(orderId, 4999, VISA));
        handler.handle(authorizeCommand(orderId, 4999, VISA));

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType).containsExactly("PaymentAuthorized", "PaymentAuthorized");
        assertThat(replies.get(1).payload()).isEqualTo(replies.get(0).payload());
        assertThat(gateway.calls(orderId + ":AUTHORIZE")).isEqualTo(1);
    }

    @Test
    void captureChargesAuthorizedAmountExactlyOnce() {
        UUID orderId = UUID.randomUUID();
        handler.handle(authorizeCommand(orderId, 12_345, VISA));

        handler.handle(captureCommand(orderId));
        handler.handle(captureCommand(orderId));

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType)
                .containsExactly("PaymentAuthorized", "PaymentCaptured", "PaymentCaptured");
        assertThat(replies.get(1).payload().get("amountCents").asLong()).isEqualTo(12_345);
        assertThat(paymentRows(orderId, "CAPTURE")).isEqualTo(1);
        assertThat(gateway.calls(orderId + ":CAPTURE")).isEqualTo(1);
    }

    @Test
    void captureWithoutSuccessfulAuthorizationFails() {
        UUID neverAuthorized = UUID.randomUUID();
        UUID declined = UUID.randomUUID();
        handler.handle(authorizeCommand(declined, 4999, MockPaymentGateway.DECLINED));

        handler.handle(captureCommand(neverAuthorized));
        handler.handle(captureCommand(declined));

        assertThat(single(replies(neverAuthorized)).eventType()).isEqualTo("CaptureFailed");
        assertThat(reason(single(replies(neverAuthorized)))).isEqualTo("NOT_AUTHORIZED");
        assertThat(reason(replies(declined).getLast())).isEqualTo("NOT_AUTHORIZED");
        assertThat(gateway.calls(neverAuthorized + ":CAPTURE")).isZero();
    }

    @Test
    void providerCaptureFailureRepliesCaptureFailed() {
        UUID orderId = UUID.randomUUID();
        handler.handle(authorizeCommand(orderId, 4999, MockPaymentGateway.CAPTURE_FAILS));

        handler.handle(captureCommand(orderId));

        EventEnvelope reply = replies(orderId).getLast();
        assertThat(reply.eventType()).isEqualTo("CaptureFailed");
        assertThat(reason(reply)).isEqualTo("CAPTURE_DECLINED");
    }

    @Test
    void voidReleasesAuthorizationAndSecondVoidIsNoOp() {
        UUID orderId = UUID.randomUUID();
        handler.handle(authorizeCommand(orderId, 4999, VISA));
        EventEnvelope firstVoid = voidCommand(orderId);

        handler.handle(firstVoid);
        assertThat(handler.handle(firstVoid)).isFalse();
        handler.handle(voidCommand(orderId));

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType)
                .containsExactly("PaymentAuthorized", "PaymentVoided", "PaymentVoided");
        assertThat(replies.get(1).payload().get("voided").asBoolean()).isTrue();
        assertThat(paymentRows(orderId, "VOID")).isEqualTo(1);
        assertThat(gateway.calls(orderId + ":VOID")).isEqualTo(1);
    }

    @Test
    void voidWithoutAuthorizationIsNoOpAndBlocksLateAuthorize() {
        UUID orderId = UUID.randomUUID();

        handler.handle(voidCommand(orderId));
        handler.handle(authorizeCommand(orderId, 4999, VISA));

        List<EventEnvelope> replies = replies(orderId);
        assertThat(replies).extracting(EventEnvelope::eventType).containsExactly("PaymentVoided", "PaymentDeclined");
        assertThat(replies.get(0).payload().get("voided").asBoolean()).isFalse();
        assertThat(reason(replies.get(1))).isEqualTo("ORDER_CANCELLED");
        assertThat(gateway.calls(orderId + ":AUTHORIZE")).isZero();
    }

    @Test
    void capturedPaymentCannotBeVoided() {
        UUID orderId = UUID.randomUUID();
        handler.handle(authorizeCommand(orderId, 4999, VISA));
        handler.handle(captureCommand(orderId));

        handler.handle(voidCommand(orderId));

        EventEnvelope reply = replies(orderId).getLast();
        assertThat(reply.eventType()).isEqualTo("PaymentVoidFailed");
        assertThat(reason(reply)).isEqualTo("ALREADY_CAPTURED");
        assertThat(gateway.calls(orderId + ":VOID")).isZero();
    }

    @Test
    void voidedAuthorizationCannotBeCaptured() {
        UUID orderId = UUID.randomUUID();
        handler.handle(authorizeCommand(orderId, 4999, VISA));
        handler.handle(voidCommand(orderId));

        handler.handle(captureCommand(orderId));

        assertThat(reason(replies(orderId).getLast())).isEqualTo("AUTHORIZATION_VOIDED");
        assertThat(gateway.calls(orderId + ":CAPTURE")).isZero();
    }

    @Test
    void concurrentCaptureCommandsRecordOneCapture() throws Exception {
        UUID orderId = UUID.randomUUID();
        handler.handle(authorizeCommand(orderId, 4999, VISA));
        int deliveries = 8;
        List<EventEnvelope> commands = new ArrayList<>();
        for (int i = 0; i < deliveries; i++) {
            commands.add(captureCommand(orderId));
        }
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Boolean>> tasks = commands.stream().<Callable<Boolean>>map(c -> () -> {
            start.await();
            return handler.handle(c);
        }).toList();

        List<EventEnvelope> losers = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(deliveries);
        try {
            List<Future<Boolean>> futures = tasks.stream().map(pool::submit).toList();
            start.countDown();
            for (int i = 0; i < deliveries; i++) {
                try {
                    futures.get(i).get();
                } catch (ExecutionException e) {
                    // Lost the race on the (order_id, operation) key; the whole transaction rolled back.
                    assertThat(e.getCause()).isInstanceOf(DuplicateKeyException.class);
                    losers.add(commands.get(i));
                }
            }
        } finally {
            pool.shutdownNow();
        }
        // Kafka would redeliver the rolled-back commands; they now see the stored capture.
        losers.forEach(handler::handle);

        assertThat(paymentRows(orderId, "CAPTURE")).isEqualTo(1);
        List<EventEnvelope> captures = replies(orderId).stream()
                .filter(r -> !r.eventType().equals("PaymentAuthorized")).toList();
        assertThat(captures).hasSize(deliveries).allMatch(r -> r.eventType().equals("PaymentCaptured"));
    }

    private static EventEnvelope single(List<EventEnvelope> replies) {
        assertThat(replies).hasSize(1);
        return replies.getFirst();
    }

    private static String reason(EventEnvelope reply) {
        return reply.payload().get("reason").asText();
    }
}
