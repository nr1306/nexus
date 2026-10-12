package com.nexus.inventory.stock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Redis is optional: when it fails, reads fall back to Postgres and writes carry on. */
class StockCacheTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final StockRepository stock = mock(StockRepository.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @Test
    void redisDownFallsBackToPostgres() {
        RedisConnectionFailureException down = new RedisConnectionFailureException("down");
        when(redis.opsForValue()).thenThrow(down);
        when(redis.delete(anyString())).thenThrow(down);
        when(stock.available("SKU-1")).thenReturn(Optional.of(5));
        StockCache cache = new StockCache(redis, new StockCacheProperties(true, Duration.ofSeconds(5)), meters);

        assertThat(new StockQueryService(stock, cache).available("SKU-1")).contains(5);
        cache.evictAfterCommit("SKU-1");

        assertThat(meters.counter("stock_cache_misses_total").count()).isEqualTo(1);
        assertThat(meters.counter("stock_cache_errors_total").count()).isEqualTo(3); // read, write, evict
    }

    @Test
    void disabledCacheNeverTouchesRedis() {
        when(stock.available("SKU-1")).thenReturn(Optional.of(5));
        StockCache cache = new StockCache(redis, new StockCacheProperties(false, Duration.ofSeconds(5)), meters);

        assertThat(new StockQueryService(stock, cache).available("SKU-1")).contains(5);
        cache.evictAfterCommit("SKU-1");

        verifyNoInteractions(redis);
    }
}
