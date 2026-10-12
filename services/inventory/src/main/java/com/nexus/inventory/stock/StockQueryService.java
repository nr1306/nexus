package com.nexus.inventory.stock;

import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.OptionalInt;

/** Stock availability for display (product reads), through the Redis cache when it is enabled. */
@Service
public class StockQueryService {

    private final StockRepository stock;
    private final StockCache cache;

    public StockQueryService(StockRepository stock, StockCache cache) {
        this.stock = stock;
        this.cache = cache;
    }

    /** @return available units, or empty for an unknown SKU */
    public Optional<Integer> available(String sku) {
        if (!cache.enabled()) {
            return stock.available(sku);
        }
        OptionalInt cached = cache.get(sku);
        if (cached.isPresent()) {
            return Optional.of(cached.getAsInt());
        }
        Optional<Integer> available = stock.available(sku);
        available.ifPresent(units -> cache.put(sku, units));
        return available;
    }
}
