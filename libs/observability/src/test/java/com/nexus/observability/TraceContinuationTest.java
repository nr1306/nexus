package com.nexus.observability;

import com.nexus.messaging.outbox.TraceparentSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.autoconfigure.observation.ObservationAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.opentelemetry.OpenTelemetryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.tracing.OpenTelemetryTracingAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Against the real OpenTelemetry bridge, as the services run it. */
class TraceContinuationTest {

    private static final String STORED = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ObservationAutoConfiguration.class, OpenTelemetryAutoConfiguration.class,
                    MicrometerTracingAutoConfiguration.class, OpenTelemetryTracingAutoConfiguration.class,
                    ObservabilityAutoConfiguration.class))
            .withPropertyValues("management.tracing.sampling.probability=1.0");

    @Test
    void continuesTheStoredTraceInANewSpan() {
        context.run(ctx -> {
            TraceContinuation traces = ctx.getBean(TraceContinuation.class);
            TraceparentSource outbox = ctx.getBean(TraceparentSource.class);

            assertThat(traces.current()).isEmpty();
            Optional<String> inside = traces.inTrace(STORED, "saga timeout", outbox::current);

            assertThat(inside).hasValueSatisfying(tp -> {
                assertThat(Traceparents.traceId(tp)).contains("4bf92f3577b34da6a3ce929d0e0e4736");
                assertThat(tp).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-01").isNotEqualTo(STORED);
            });
            assertThat(traces.current()).isEmpty();
        });
    }

    @Test
    void withoutAStoredTraceStartsANewOne() {
        context.run(ctx -> {
            TraceContinuation traces = ctx.getBean(TraceContinuation.class);

            Optional<String> inside = traces.inTrace(null, "fraud check", traces::current);

            assertThat(inside).hasValueSatisfying(tp -> assertThat(Traceparents.traceId(tp)).isPresent()
                    .get().isNotEqualTo("4bf92f3577b34da6a3ce929d0e0e4736"));
        });
    }

    @Test
    void withoutTracingTheWorkStillRuns() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ObservabilityAutoConfiguration.class))
                .run(ctx -> {
                    TraceContinuation traces = ctx.getBean(TraceContinuation.class);
                    assertThat(traces.inTrace(STORED, "work", () -> "done")).isEqualTo("done");
                    assertThat(ctx.getBean(TraceparentSource.class).current()).isEmpty();
                });
    }

    @Test
    void traceIdParsing() {
        assertThat(Traceparents.traceId(STORED)).contains("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(Traceparents.traceId("garbage")).isEmpty();
        assertThat(Traceparents.traceId(null)).isEmpty();
    }
}
