package com.nexus.payment.gateway.stripe;

/** Transient: timeout, connection error, 429 or 5xx, or the circuit is open. Thrown, so the command is retried. */
public class StripeUnavailableException extends RuntimeException {

    public StripeUnavailableException(String message) {
        super(message);
    }

    public StripeUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
