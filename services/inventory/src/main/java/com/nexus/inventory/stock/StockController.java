package com.nexus.inventory.stock;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Read-only availability for display; may be up to the cache TTL old. Reservations go through Kafka. */
@RestController
@RequestMapping("/stock")
class StockController {

    record Availability(String sku, int available) {
    }

    private final StockQueryService stockQueries;

    StockController(StockQueryService stockQueries) {
        this.stockQueries = stockQueries;
    }

    @GetMapping("/{sku}")
    ResponseEntity<Availability> available(@PathVariable String sku) {
        return stockQueries.available(sku)
                .map(units -> ResponseEntity.ok(new Availability(sku, units)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
