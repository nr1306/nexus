package com.nexus.inventory.stock;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Redis read cache for stock availability (SPEC.md §6).
 *
 * @param enabled false reads Postgres every time (S1 compares cache on vs off)
 * @param ttl     how long a cached value lives; it is also evicted after every change
 */
@ConfigurationProperties("nexus.inventory.stock-cache")
public record StockCacheProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("PT5S") Duration ttl) {
}
