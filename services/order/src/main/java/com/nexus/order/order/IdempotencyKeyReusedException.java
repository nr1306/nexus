package com.nexus.order.order;

/** The Idempotency-Key was already used with a different request body (HTTP 422). */
public class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException(String key) {
        super("Idempotency-Key '" + key + "' was already used with a different request");
    }
}
