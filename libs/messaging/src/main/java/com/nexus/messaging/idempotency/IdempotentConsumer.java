package com.nexus.messaging.idempotency;

import com.nexus.messaging.envelope.EventEnvelope;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * Runs a side effect at most once per (consumer group, event id) (CLAUDE.md rule 2).
 *
 * <p>The {@code processed_events} insert and the side effect share one transaction: if the side
 * effect fails, the marker rolls back too and a redelivery processes the event again. Concurrent
 * duplicates block on the primary key until the first transaction finishes, then skip.
 */
public class IdempotentConsumer {

    private static final String MARK_PROCESSED = """
            INSERT INTO processed_events (consumer_group, event_id)
            VALUES (?, ?)
            ON CONFLICT DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;

    public IdempotentConsumer(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate,
                              MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.meterRegistry = meterRegistry;
    }

    /**
     * @return {@code true} if the side effect ran, {@code false} if the event was a duplicate
     */
    public boolean handle(String consumerGroup, EventEnvelope envelope, Consumer<EventEnvelope> sideEffect) {
        return handle(consumerGroup, envelope.eventId(), () -> sideEffect.accept(envelope));
    }

    public boolean handle(String consumerGroup, UUID eventId, Runnable sideEffect) {
        Boolean processed = transactionTemplate.execute(status -> {
            int inserted = jdbcTemplate.update(MARK_PROCESSED, consumerGroup, eventId);
            if (inserted == 0) {
                return false;
            }
            sideEffect.run();
            return true;
        });
        if (!Boolean.TRUE.equals(processed)) {
            duplicatesSkipped(consumerGroup).increment();
            return false;
        }
        return true;
    }

    private Counter duplicatesSkipped(String consumerGroup) {
        return Counter.builder("duplicate_events_skipped")
                .description("Events skipped because this consumer group already processed them")
                .tag("consumer_group", consumerGroup)
                .register(meterRegistry);
    }
}
