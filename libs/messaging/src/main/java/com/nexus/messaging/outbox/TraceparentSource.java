package com.nexus.messaging.outbox;

import java.util.Optional;

/**
 * Supplies the W3C traceparent of the current span, if any, for the outbox row.
 * The default returns empty; Phase 4 replaces it with an OpenTelemetry-backed bean.
 */
@FunctionalInterface
public interface TraceparentSource {

    Optional<String> current();

    static TraceparentSource none() {
        return Optional::empty;
    }
}
