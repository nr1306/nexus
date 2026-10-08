package com.nexus.messaging.retry;

import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffInterruptedException;
import org.springframework.retry.backoff.Sleeper;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.ThreadWaitSleeper;

import java.time.Duration;
import java.util.List;

/**
 * Back-off with an explicit list of delays (e.g. 1 s, 5 s, 30 s), which Spring Kafka turns into one retry
 * topic per delay. Exponential back-off can't produce that exact sequence.
 */
public class FixedDelaysBackOffPolicy implements SleepingBackOffPolicy<FixedDelaysBackOffPolicy> {

    private final List<Duration> delays;
    private final Sleeper sleeper;

    public FixedDelaysBackOffPolicy(List<Duration> delays) {
        this(delays, new ThreadWaitSleeper());
    }

    private FixedDelaysBackOffPolicy(List<Duration> delays, Sleeper sleeper) {
        if (delays.isEmpty()) {
            throw new IllegalArgumentException("At least one retry delay is required");
        }
        this.delays = List.copyOf(delays);
        this.sleeper = sleeper;
    }

    @Override
    public FixedDelaysBackOffPolicy withSleeper(Sleeper sleeper) {
        return new FixedDelaysBackOffPolicy(delays, sleeper);
    }

    @Override
    public BackOffContext start(RetryContext context) {
        return new Attempt();
    }

    @Override
    public void backOff(BackOffContext backOffContext) throws BackOffInterruptedException {
        Attempt attempt = (Attempt) backOffContext;
        Duration delay = delays.get(Math.min(attempt.index++, delays.size() - 1));
        try {
            sleeper.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BackOffInterruptedException("Interrupted during back-off", e);
        }
    }

    private static final class Attempt implements BackOffContext {
        private int index;
    }
}
