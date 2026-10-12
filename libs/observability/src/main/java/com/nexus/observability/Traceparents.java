package com.nexus.observability;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;

import java.util.Optional;

/** W3C {@code traceparent} formatting: {@code 00-<trace id>-<span id>-<flags>}. */
public final class Traceparents {

    private Traceparents() {
    }

    /** @return the span's traceparent, or empty for no span or a no-op span */
    public static Optional<String> of(Span span) {
        if (span == null || span.isNoop()) {
            return Optional.empty();
        }
        TraceContext context = span.context();
        if (context.traceId().isEmpty() || context.spanId().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of("00-" + context.traceId() + "-" + context.spanId() + "-"
                + (Boolean.TRUE.equals(context.sampled()) ? "01" : "00"));
    }

    /** @return the trace id of a traceparent, or empty if it is malformed */
    public static Optional<String> traceId(String traceparent) {
        if (traceparent == null) {
            return Optional.empty();
        }
        String[] parts = traceparent.split("-");
        return parts.length == 4 && parts[1].length() == 32 ? Optional.of(parts[1]) : Optional.empty();
    }
}
