package com.nexus.payment.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.outbox.OutboxWriter;
import com.nexus.payment.gateway.PaymentGateway;
import com.nexus.payment.gateway.PaymentGateway.AuthorizationResult;
import com.nexus.payment.gateway.PaymentGateway.OperationResult;
import com.nexus.payment.messaging.PaymentTopics;
import com.nexus.payment.payment.PaymentMessages.AuthorizePayment;
import com.nexus.payment.payment.PaymentMessages.CaptureFailed;
import com.nexus.payment.payment.PaymentMessages.PaymentAuthorized;
import com.nexus.payment.payment.PaymentMessages.PaymentCaptured;
import com.nexus.payment.payment.PaymentMessages.PaymentDeclined;
import com.nexus.payment.payment.PaymentMessages.PaymentRefundFailed;
import com.nexus.payment.payment.PaymentMessages.PaymentRefunded;
import com.nexus.payment.payment.PaymentMessages.PaymentVoidFailed;
import com.nexus.payment.payment.PaymentMessages.PaymentVoided;
import com.nexus.payment.payment.PaymentRecord.Operation;
import com.nexus.payment.payment.PaymentRecord.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Authorize, capture, void and refund, each at most once per order. Every method runs inside the caller's
 * idempotent-consumer transaction: the payments row and the reply in the outbox commit together.
 * The provider call happens inside that transaction with an idempotency key, so if the transaction
 * rolls back after the provider acted, the redelivery gets the provider's original result (ADR 0002).
 *
 * <p>Every outcome, including rejections, is stored as a row first and the reply is built from the
 * row, so a retried command (new eventId, same order) gets exactly the same answer.
 */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String AGGREGATE_TYPE = "payment";
    private static final int SCHEMA_VERSION = 1;
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");

    private final PaymentRepository payments;
    private final PaymentGateway gateway;
    private final OutboxWriter outbox;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public PaymentService(PaymentRepository payments, PaymentGateway gateway, OutboxWriter outbox,
                          ObjectMapper objectMapper, Clock clock) {
        this.payments = payments;
        this.gateway = gateway;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public void authorize(EventEnvelope command) {
        UUID orderId = command.orderId();
        PaymentRecord record = payments.find(orderId, Operation.AUTHORIZE).orElseGet(() -> {
            if (payments.find(orderId, Operation.VOID).isPresent()) {
                return failed(orderId, Operation.AUTHORIZE, PaymentMessages.ORDER_CANCELLED);
            }
            AuthorizePayment request = parseAuthorize(command);
            if (request == null) {
                return failed(orderId, Operation.AUTHORIZE, PaymentMessages.INVALID_REQUEST);
            }
            AuthorizationResult result = gateway.authorize(PaymentRecord.idempotencyKey(orderId, Operation.AUTHORIZE),
                    request.amountCents(), request.currency(), request.paymentMethod());
            return save(new PaymentRecord(orderId, Operation.AUTHORIZE,
                    result.approved() ? Status.SUCCEEDED : Status.FAILED,
                    request.amountCents(), request.currency(), gateway.name(),
                    result.authorizationId(), result.declineReason()));
        });
        reply(command, record);
    }

    public void capture(EventEnvelope command) {
        UUID orderId = command.orderId();
        PaymentRecord record = payments.find(orderId, Operation.CAPTURE).orElseGet(() -> {
            Optional<PaymentRecord> authorization = payments.find(orderId, Operation.AUTHORIZE).filter(PaymentRecord::succeeded);
            if (authorization.isEmpty()) {
                return failed(orderId, Operation.CAPTURE, PaymentMessages.NOT_AUTHORIZED);
            }
            if (payments.find(orderId, Operation.VOID).filter(PaymentRecord::succeeded).isPresent()) {
                return failed(orderId, Operation.CAPTURE, PaymentMessages.AUTHORIZATION_VOIDED);
            }
            // Any refund outcome, even SKIPPED, means the saga is compensating: a capture arriving late
            // (e.g. from a retry topic) must not charge the card after the refund step has run.
            if (payments.find(orderId, Operation.REFUND).isPresent()) {
                return failed(orderId, Operation.CAPTURE, PaymentMessages.ORDER_CANCELLED);
            }
            PaymentRecord auth = authorization.get();
            OperationResult result = gateway.capture(PaymentRecord.idempotencyKey(orderId, Operation.CAPTURE),
                    auth.providerRef(), auth.amountCents(), auth.currency());
            return save(new PaymentRecord(orderId, Operation.CAPTURE,
                    result.succeeded() ? Status.SUCCEEDED : Status.FAILED,
                    auth.amountCents(), auth.currency(), gateway.name(), auth.providerRef(), result.failureReason()));
        });
        reply(command, record);
    }

    /**
     * Compensation. Voiding twice, or voiding an order without a successful authorization, is a no-op
     * that still replies (voided=false). A captured payment can't be voided: if it was refunded the
     * void is a no-op too, otherwise it fails and the saga must refund.
     */
    public void voidPayment(EventEnvelope command) {
        UUID orderId = command.orderId();
        PaymentRecord record = payments.find(orderId, Operation.VOID).orElseGet(() -> {
            if (payments.find(orderId, Operation.CAPTURE).filter(PaymentRecord::succeeded).isPresent()) {
                if (payments.find(orderId, Operation.REFUND).filter(PaymentRecord::succeeded).isPresent()) {
                    return save(new PaymentRecord(orderId, Operation.VOID, Status.SKIPPED,
                            null, null, gateway.name(), null, null));
                }
                return failed(orderId, Operation.VOID, PaymentMessages.ALREADY_CAPTURED);
            }
            Optional<PaymentRecord> authorization = payments.find(orderId, Operation.AUTHORIZE).filter(PaymentRecord::succeeded);
            if (authorization.isEmpty()) {
                return save(new PaymentRecord(orderId, Operation.VOID, Status.SKIPPED,
                        null, null, gateway.name(), null, null));
            }
            PaymentRecord auth = authorization.get();
            OperationResult result = gateway.voidAuthorization(PaymentRecord.idempotencyKey(orderId, Operation.VOID),
                    auth.providerRef());
            if (!result.succeeded()) {
                // Not a business outcome the saga can act on; roll back and let redelivery retry.
                throw new IllegalStateException("Provider failed to void authorization: " + result.failureReason());
            }
            return save(new PaymentRecord(orderId, Operation.VOID, Status.SUCCEEDED,
                    auth.amountCents(), auth.currency(), gateway.name(), auth.providerRef(), null));
        });
        reply(command, record);
    }

    /**
     * Compensation. Refunds the captured amount in full. Refunding twice, or refunding an order without
     * a successful capture, is a no-op that still replies (refunded=false); the stored row then blocks a
     * late capture. A refund the provider refuses is stored as FAILED and replied (the saga needs an
     * operator); a provider error is thrown and retried.
     */
    public void refund(EventEnvelope command) {
        UUID orderId = command.orderId();
        PaymentRecord record = payments.find(orderId, Operation.REFUND).orElseGet(() -> {
            Optional<PaymentRecord> capture = payments.find(orderId, Operation.CAPTURE).filter(PaymentRecord::succeeded);
            if (capture.isEmpty()) {
                return save(new PaymentRecord(orderId, Operation.REFUND, Status.SKIPPED,
                        null, null, gateway.name(), null, null));
            }
            PaymentRecord captured = capture.get();
            OperationResult result = gateway.refund(PaymentRecord.idempotencyKey(orderId, Operation.REFUND),
                    captured.providerRef(), captured.amountCents(), captured.currency());
            return save(new PaymentRecord(orderId, Operation.REFUND,
                    result.succeeded() ? Status.SUCCEEDED : Status.FAILED,
                    captured.amountCents(), captured.currency(), gateway.name(), captured.providerRef(),
                    result.failureReason()));
        });
        reply(command, record);
    }

    private PaymentRecord failed(UUID orderId, Operation operation, String reason) {
        return save(new PaymentRecord(orderId, operation, Status.FAILED, null, null, gateway.name(), null, reason));
    }

    private PaymentRecord save(PaymentRecord record) {
        payments.insert(record);
        log.info("{} {}{}", record.operation(), record.status(),
                record.failureReason() == null ? "" : " (" + record.failureReason() + ")");
        return record;
    }

    private void reply(EventEnvelope command, PaymentRecord record) {
        String eventType;
        Object payload;
        switch (record.operation()) {
            case AUTHORIZE -> {
                if (record.succeeded()) {
                    eventType = PaymentMessages.PAYMENT_AUTHORIZED;
                    payload = new PaymentAuthorized(record.amountCents(), record.currency(), record.providerRef());
                } else {
                    eventType = PaymentMessages.PAYMENT_DECLINED;
                    payload = new PaymentDeclined(record.failureReason());
                }
            }
            case CAPTURE -> {
                if (record.succeeded()) {
                    eventType = PaymentMessages.PAYMENT_CAPTURED;
                    payload = new PaymentCaptured(record.amountCents(), record.currency());
                } else {
                    eventType = PaymentMessages.CAPTURE_FAILED;
                    payload = new CaptureFailed(record.failureReason());
                }
            }
            case VOID -> {
                if (record.status() == Status.FAILED) {
                    eventType = PaymentMessages.PAYMENT_VOID_FAILED;
                    payload = new PaymentVoidFailed(record.failureReason());
                } else {
                    eventType = PaymentMessages.PAYMENT_VOIDED;
                    payload = new PaymentVoided(record.succeeded());
                }
            }
            case REFUND -> {
                if (record.status() == Status.FAILED) {
                    eventType = PaymentMessages.PAYMENT_REFUND_FAILED;
                    payload = new PaymentRefundFailed(record.failureReason());
                } else {
                    eventType = PaymentMessages.PAYMENT_REFUNDED;
                    payload = new PaymentRefunded(record.succeeded(),
                            record.succeeded() ? record.amountCents() : null,
                            record.succeeded() ? record.currency() : null);
                }
            }
            default -> throw new IllegalStateException("No reply defined for " + record.operation());
        }
        EventEnvelope event = EventEnvelope.create(eventType, SCHEMA_VERSION, command.orderId(), command.sagaId(),
                objectMapper.valueToTree(payload), clock);
        outbox.append(PaymentTopics.EVENTS, AGGREGATE_TYPE, event);
    }

    /** @return the request, or {@code null} if the payload is not a valid AuthorizePayment */
    private AuthorizePayment parseAuthorize(EventEnvelope command) {
        AuthorizePayment request;
        try {
            request = objectMapper.treeToValue(command.payload(), AuthorizePayment.class);
        } catch (JsonProcessingException e) {
            return null;
        }
        boolean valid = request.amountCents() != null && request.amountCents() > 0
                && request.currency() != null && CURRENCY.matcher(request.currency()).matches()
                && request.paymentMethod() != null && !request.paymentMethod().isBlank();
        return valid ? request : null;
    }
}
