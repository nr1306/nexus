package com.nexus.inventory.stock;

import com.nexus.inventory.InventoryIntegrationTest;
import com.nexus.inventory.messaging.InventoryCommandHandler;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Cache-aside stock reads against real Redis and Postgres (SPEC.md §6). */
class StockCacheIT extends InventoryIntegrationTest {

    @Autowired
    StockQueryService stockQueries;

    @Autowired
    InventoryCommandHandler handler;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    void readIsCachedAndEvictedWhenAReservationCommits() {
        String sku = newSku(10);
        double hits = counter("stock_cache_hits_total");
        double misses = counter("stock_cache_misses_total");

        assertThat(stockQueries.available(sku)).contains(10);
        assertThat(redis.opsForValue().get("stock:available:" + sku)).isEqualTo("10");
        assertThat(redis.getExpire("stock:available:" + sku)).isBetween(1L, 5L);
        assertThat(stockQueries.available(sku)).contains(10);
        assertThat(counter("stock_cache_misses_total") - misses).isEqualTo(1);
        assertThat(counter("stock_cache_hits_total") - hits).isEqualTo(1);

        UUID orderId = UUID.randomUUID();
        handler.handle(reserveCommand(orderId, Map.of(sku, 3)));
        assertThat(redis.hasKey("stock:available:" + sku)).isFalse();
        assertThat(stockQueries.available(sku)).contains(7);

        handler.handle(releaseCommand(orderId));
        assertThat(redis.hasKey("stock:available:" + sku)).isFalse();
        assertThat(stockQueries.available(sku)).contains(10);
    }

    @Test
    void rejectedReservationLeavesTheCacheAlone() {
        String sku = newSku(1);
        stockQueries.available(sku);

        handler.handle(reserveCommand(UUID.randomUUID(), Map.of(sku, 2)));

        assertThat(redis.opsForValue().get("stock:available:" + sku)).isEqualTo("1");
    }

    @Test
    void unknownSkuIsNotFoundAndNotCached() throws Exception {
        String sku = "NO-SUCH-SKU-" + UUID.randomUUID();
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new StockController(stockQueries)).build();

        mvc.perform(get("/stock/{sku}", sku)).andExpect(status().isNotFound());
        assertThat(redis.hasKey("stock:available:" + sku)).isFalse();

        String known = newSku(4);
        mvc.perform(get("/stock/{sku}", known)).andExpect(status().isOk())
                .andExpect(jsonPath("$.sku").value(known))
                .andExpect(jsonPath("$.available").value(4));
    }

    private double counter(String name) {
        return meterRegistry.counter(name).count();
    }
}
