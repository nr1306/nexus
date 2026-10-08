package com.nexus.fraud.decision;

import com.nexus.fraud.config.FraudProperties;
import com.nexus.fraud.rules.FraudRules;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * Evaluates an order once and stores the decision; a repeated request for the same order returns the
 * stored decision even if the facts have changed since (e.g. the customer placed more orders).
 */
@Service
public class FraudService {

    private static final Logger log = LoggerFactory.getLogger(FraudService.class);

    private final DecisionRepository decisions;
    private final FraudProperties properties;
    private final MeterRegistry meterRegistry;
    private final Clock clock;

    public FraudService(DecisionRepository decisions, FraudProperties properties, MeterRegistry meterRegistry, Clock clock) {
        this.decisions = decisions;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.clock = clock;
    }

    @Transactional
    public FraudDecision evaluate(EvaluationRequest request) {
        var existing = decisions.find(request.orderId());
        if (existing.isPresent()) {
            log.info("Repeated evaluation; returning stored decision");
            return existing.get();
        }

        Instant now = clock.instant();
        var facts = new FraudRules.Facts(
                request.amountCents(),
                decisions.countRecent(request.customerId(), now.minus(properties.velocityWindow()), request.orderId()),
                decisions.isBlocked("CUSTOMER", request.customerId()),
                decisions.isBlocked("PAYMENT_METHOD", request.paymentMethod()));
        List<String> reasons = FraudRules.evaluate(facts,
                new FraudRules.Limits(properties.maxAmountCents(), properties.velocityMaxOrders()));
        FraudDecision decision = new FraudDecision(request.orderId(), reasons.isEmpty(), reasons);

        if (!decisions.insert(request, decision, now)) {
            // A concurrent evaluation of the same order committed first; its decision stands.
            return decisions.find(request.orderId()).orElseThrow();
        }
        meterRegistry.counter("fraud_decisions", "decision", decision.approved() ? "APPROVE" : "REJECT").increment();
        log.info("{} {}", decision.approved() ? "APPROVE" : "REJECT", reasons);
        return decision;
    }
}
