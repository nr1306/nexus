package com.nexus.order;

import com.nexus.order.fraud.FraudClient;
import com.nexus.order.fraud.FraudUnavailableException;
import com.nexus.order.order.OrderDetails;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Stands in for the Fraud service in Order tests. Defaults to APPROVE. */
public class StubFraudClient implements FraudClient {

    public enum Mode { APPROVE, REJECT, UNAVAILABLE }

    private volatile Mode mode = Mode.APPROVE;
    public final AtomicInteger calls = new AtomicInteger();

    public void set(Mode mode) {
        this.mode = mode;
    }

    @Override
    public Verdict evaluate(OrderDetails order, UUID sagaId) {
        calls.incrementAndGet();
        return switch (mode) {
            case APPROVE -> new Verdict(true, List.of());
            case REJECT -> new Verdict(false, List.of("VELOCITY"));
            case UNAVAILABLE -> throw new FraudUnavailableException("stubbed outage", null);
        };
    }
}
