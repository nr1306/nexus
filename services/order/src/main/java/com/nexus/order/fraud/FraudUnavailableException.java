package com.nexus.order.fraud;

public class FraudUnavailableException extends RuntimeException {

    public FraudUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
