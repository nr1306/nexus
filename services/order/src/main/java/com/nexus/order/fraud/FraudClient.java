package com.nexus.order.fraud;

import com.nexus.order.order.OrderDetails;

import java.util.List;
import java.util.UUID;

/** Port to the Fraud service. */
public interface FraudClient {

    record Verdict(boolean approved, List<String> reasons) {
    }

    /**
     * @throws FraudUnavailableException if no decision could be obtained (timeout, error, breaker open);
     *                                   the saga retries until its step deadline, then compensates
     */
    Verdict evaluate(OrderDetails order, UUID sagaId);
}
