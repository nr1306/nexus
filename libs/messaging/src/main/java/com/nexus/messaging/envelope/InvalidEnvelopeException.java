package com.nexus.messaging.envelope;

/**
 * A message that cannot be parsed as an envelope. Permanent: retrying will not help, so it goes
 * straight to the DLQ (CLAUDE.md rule 10).
 */
public class InvalidEnvelopeException extends RuntimeException {

    public InvalidEnvelopeException(String message, Throwable cause) {
        super(message, cause);
    }
}
