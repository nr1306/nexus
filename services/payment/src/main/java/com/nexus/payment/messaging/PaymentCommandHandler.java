package com.nexus.payment.messaging;

import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.idempotency.IdempotentConsumer;
import com.nexus.payment.payment.PaymentMessages;
import com.nexus.payment.payment.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * Applies one payment command at most once (CLAUDE.md rule 2).
 */
@Component
public class PaymentCommandHandler {

    public static final String CONSUMER_GROUP = "payment-svc";

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandHandler.class);

    private final IdempotentConsumer idempotentConsumer;
    private final PaymentService paymentService;

    public PaymentCommandHandler(IdempotentConsumer idempotentConsumer, PaymentService paymentService) {
        this.idempotentConsumer = idempotentConsumer;
        this.paymentService = paymentService;
    }

    /** @return {@code false} if the command was a duplicate and was skipped */
    public boolean handle(EventEnvelope command) {
        try (var orderId = MDC.putCloseable("orderId", command.orderId().toString());
             var sagaId = MDC.putCloseable("sagaId", command.sagaId().toString());
             var eventId = MDC.putCloseable("eventId", command.eventId().toString());
             var eventType = MDC.putCloseable("eventType", command.eventType())) {
            boolean processed = idempotentConsumer.handle(CONSUMER_GROUP, command, this::apply);
            if (!processed) {
                log.info("Skipped duplicate command");
            }
            return processed;
        }
    }

    private void apply(EventEnvelope command) {
        switch (command.eventType()) {
            case PaymentMessages.AUTHORIZE_PAYMENT -> paymentService.authorize(command);
            case PaymentMessages.CAPTURE_PAYMENT -> paymentService.capture(command);
            case PaymentMessages.VOID_PAYMENT -> paymentService.voidPayment(command);
            default -> log.warn("Ignoring unknown command type");
        }
    }
}
