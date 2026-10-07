package com.nexus.order.order;

/** The request can't be accepted as an order (HTTP 400). */
public class InvalidOrderException extends RuntimeException {

    public InvalidOrderException(String message) {
        super(message);
    }
}
