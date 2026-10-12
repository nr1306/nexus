package com.nexus.observability;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Runs work in a span that continues a stored trace. For work no request or message carries a trace
 * into: scheduled sweepers and pollers that act on an order (Order's saga timeouts and fraud checks
 * store the order's traceparent with the saga). Without a stored traceparent the span starts a new trace.
 */
public class TraceContinuation {

    private final Tracer tracer;
    private final Propagator propagator;

    public TraceContinuation(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** The current span's traceparent, to store with the order. */
    public Optional<String> current() {
        return Traceparents.of(tracer.currentSpan());
    }

    public <T> T inTrace(String traceparent, String spanName, Supplier<T> work) {
        Span span = (traceparent == null
                ? tracer.spanBuilder()
                : propagator.extract(Map.of("traceparent", traceparent), (carrier, key) -> carrier.get(key)))
                .name(spanName)
                .start();
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            return work.get();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    public void inTrace(String traceparent, String spanName, Runnable work) {
        inTrace(traceparent, spanName, () -> {
            work.run();
            return null;
        });
    }
}
