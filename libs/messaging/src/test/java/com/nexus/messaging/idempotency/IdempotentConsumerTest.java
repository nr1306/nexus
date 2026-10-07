package com.nexus.messaging.idempotency;

import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.support.PostgresTestSupport;
import com.nexus.messaging.support.TestEnvelopes;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotentConsumerTest extends PostgresTestSupport {

    private static final String GROUP = "inventory-svc";

    private SimpleMeterRegistry meterRegistry;
    private IdempotentConsumer consumer;

    @BeforeAll
    static void createSideEffectTable() {
        JDBC.execute("CREATE TABLE IF NOT EXISTS side_effects (id bigserial PRIMARY KEY, event_id uuid NOT NULL)");
    }

    @BeforeEach
    void setUp() {
        JDBC.execute("TRUNCATE side_effects");
        meterRegistry = new SimpleMeterRegistry();
        consumer = new IdempotentConsumer(JDBC, TX, meterRegistry);
    }

    @Test
    void sameEventDeliveredTwiceHasOneSideEffect() {
        EventEnvelope event = TestEnvelopes.reserveInventory(UUID.randomUUID());

        boolean first = consumer.handle(GROUP, event, this::recordSideEffect);
        boolean second = consumer.handle(GROUP, event, this::recordSideEffect);

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(sideEffectCount(event)).isEqualTo(1);
        assertThat(meterRegistry.get("duplicate_events_skipped").tag("consumer_group", GROUP).counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void failedSideEffectRollsBackMarkerSoRedeliveryIsProcessed() {
        EventEnvelope event = TestEnvelopes.reserveInventory(UUID.randomUUID());

        assertThatThrownBy(() -> consumer.handle(GROUP, event, e -> {
            recordSideEffect(e);
            throw new IllegalStateException("downstream failure");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(sideEffectCount(event)).isZero();
        assertThat(processedCount(event)).isZero();

        assertThat(consumer.handle(GROUP, event, this::recordSideEffect)).isTrue();
        assertThat(sideEffectCount(event)).isEqualTo(1);
    }

    @Test
    void differentConsumerGroupsEachProcessTheEventOnce() {
        EventEnvelope event = TestEnvelopes.reserveInventory(UUID.randomUUID());

        assertThat(consumer.handle("order-saga", event, this::recordSideEffect)).isTrue();
        assertThat(consumer.handle("reconciliation", event, this::recordSideEffect)).isTrue();
        assertThat(consumer.handle("order-saga", event, this::recordSideEffect)).isFalse();

        assertThat(sideEffectCount(event)).isEqualTo(2);
    }

    @Test
    void concurrentDuplicatesHaveExactlyOneSideEffect() throws Exception {
        EventEnvelope event = TestEnvelopes.reserveInventory(UUID.randomUUID());
        int deliveries = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < deliveries; i++) {
            tasks.add(() -> {
                start.await();
                return consumer.handle(GROUP, event, this::recordSideEffect);
            });
        }

        ExecutorService pool = Executors.newFixedThreadPool(deliveries);
        try {
            List<Future<Boolean>> futures = tasks.stream().map(pool::submit).toList();
            start.countDown();
            long processed = 0;
            for (Future<Boolean> f : futures) {
                if (f.get()) {
                    processed++;
                }
            }
            assertThat(processed).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }

        assertThat(sideEffectCount(event)).isEqualTo(1);
    }

    private void recordSideEffect(EventEnvelope event) {
        JDBC.update("INSERT INTO side_effects (event_id) VALUES (?)", event.eventId());
    }

    private int sideEffectCount(EventEnvelope event) {
        return JDBC.queryForObject("SELECT count(*) FROM side_effects WHERE event_id = ?", Integer.class, event.eventId());
    }

    private int processedCount(EventEnvelope event) {
        return JDBC.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Integer.class, event.eventId());
    }
}
