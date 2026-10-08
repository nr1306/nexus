package com.nexus.fraud.grpc;

import com.nexus.contracts.fraud.v1.Decision;
import com.nexus.contracts.fraud.v1.EvaluateRequest;
import com.nexus.contracts.fraud.v1.EvaluateResponse;
import com.nexus.contracts.fraud.v1.FraudServiceGrpc;
import com.nexus.fraud.decision.EvaluationRequest;
import com.nexus.fraud.decision.FraudDecision;
import com.nexus.fraud.decision.FraudService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.regex.Pattern;

/** gRPC endpoint for {@code nexus.fraud.v1.FraudService} (contracts/proto). */
@Component
public class FraudGrpcService extends FraudServiceGrpc.FraudServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(FraudGrpcService.class);
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");

    private final FraudService fraudService;

    public FraudGrpcService(FraudService fraudService) {
        this.fraudService = fraudService;
    }

    @Override
    public void evaluate(EvaluateRequest request, StreamObserver<EvaluateResponse> responseObserver) {
        try (var orderId = MDC.putCloseable("orderId", request.getOrderId());
             var sagaId = MDC.putCloseable("sagaId", request.getSagaId())) {
            EvaluationRequest evaluation;
            try {
                evaluation = toEvaluation(request);
            } catch (IllegalArgumentException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
                return;
            }
            try {
                FraudDecision decision = fraudService.evaluate(evaluation);
                responseObserver.onNext(EvaluateResponse.newBuilder()
                        .setDecision(decision.approved() ? Decision.APPROVE : Decision.REJECT)
                        .addAllReasons(decision.reasons())
                        .build());
                responseObserver.onCompleted();
            } catch (RuntimeException e) {
                log.error("Evaluation failed", e);
                responseObserver.onError(Status.INTERNAL.withDescription("evaluation failed").asRuntimeException());
            }
        }
    }

    private static EvaluationRequest toEvaluation(EvaluateRequest r) {
        UUID orderId = uuid(r.getOrderId(), "order_id");
        UUID sagaId = uuid(r.getSagaId(), "saga_id");
        if (r.getCustomerId().isBlank()) {
            throw new IllegalArgumentException("customer_id is required");
        }
        if (r.getAmountCents() <= 0) {
            throw new IllegalArgumentException("amount_cents must be positive");
        }
        if (!CURRENCY.matcher(r.getCurrency()).matches()) {
            throw new IllegalArgumentException("currency must be an ISO 4217 code");
        }
        if (r.getPaymentMethod().isBlank()) {
            throw new IllegalArgumentException("payment_method is required");
        }
        return new EvaluationRequest(orderId, sagaId, r.getCustomerId(), r.getAmountCents(), r.getCurrency(), r.getPaymentMethod());
    }

    private static UUID uuid(String value, String field) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(field + " must be a UUID");
        }
    }
}
