package com.nexus.inventory.stock;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.OptionalInt;

/**
 * Cache-aside store for a SKU's available stock (SPEC.md §6): display only. Reservations never read it;
 * they use the conditional update in {@link StockRepository} (CLAUDE.md rule 5), so a stale or missing
 * value can't cause overselling.
 *
 * <p>Redis is optional: any Redis error counts as a miss (reads fall back to Postgres) and is counted
 * in {@code stock_cache_errors_total}. Entries are evicted after the transaction that changed the stock
 * commits; a reader racing that commit can re-cache the old value, which the TTL bounds.
 */
@Component
public class StockCache {

    private static final Logger log = LoggerFactory.getLogger(StockCache.class);
    private static final String PREFIX = "stock:available:";

    private final StringRedisTemplate redis;
    private final StockCacheProperties properties;
    private final Counter hits;
    private final Counter misses;
    private final Counter errors;

    public StockCache(StringRedisTemplate redis, StockCacheProperties properties, MeterRegistry meterRegistry) {
        this.redis = redis;
        this.properties = properties;
        this.hits = meterRegistry.counter("stock_cache_hits_total");
        this.misses = meterRegistry.counter("stock_cache_misses_total");
        this.errors = meterRegistry.counter("stock_cache_errors_total");
    }

    public boolean enabled() {
        return properties.enabled();
    }

    /** The cached value, or empty on a miss or Redis error. */
    public OptionalInt get(String sku) {
        String value;
        try {
            value = redis.opsForValue().get(PREFIX + sku);
        } catch (RuntimeException e) {
            failed("read", e);
            misses.increment();
            return OptionalInt.empty();
        }
        if (value == null) {
            misses.increment();
            return OptionalInt.empty();
        }
        hits.increment();
        return OptionalInt.of(Integer.parseInt(value));
    }

    public void put(String sku, int available) {
        try {
            redis.opsForValue().set(PREFIX + sku, Integer.toString(available), properties.ttl());
        } catch (RuntimeException e) {
            failed("write", e);
        }
    }

    /** Evicts the SKU once the current transaction commits (at once if there is none). */
    public void evictAfterCommit(String sku) {
        if (!properties.enabled()) {
            return;
        }
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            evict(sku);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                evict(sku);
            }
        });
    }

    private void evict(String sku) {
        try {
            redis.delete(PREFIX + sku);
        } catch (RuntimeException e) {
            failed("evict", e);
        }
    }

    private void failed(String operation, RuntimeException e) {
        errors.increment();
        log.debug("Stock cache {} failed: {}", operation, e.toString());
    }
}
