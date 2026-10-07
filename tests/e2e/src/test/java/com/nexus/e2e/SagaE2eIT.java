package com.nexus.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Real Order, Inventory and Payment services talking over Kafka via Debezium.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SagaE2eIT extends E2eSupport {

    @Test
    @Order(1)
    void happyPathCompletesAndSettlesStockAndPayment() {
        String sku = newSku(10);

        UUID orderId = placeOrder("pm_card_visa", Map.of(sku, 3));

        assertThat(awaitTerminal(orderId)).isEqualTo("COMPLETED");
        // CommitInventory is fire-and-forget; wait for Inventory to settle the hold.
        await().atMost(Duration.ofSeconds(30)).until(() -> reserved(sku) == 0);
        assertThat(available(sku)).isEqualTo(7);
        assertThat(payments(orderId)).containsExactlyInAnyOrder("AUTHORIZE:SUCCEEDED", "CAPTURE:SUCCEEDED");
    }

    @Test
    @Order(2)
    void outOfStockCancelsWithoutTouchingPayment() {
        String sku = newSku(1);

        UUID orderId = placeOrder("pm_card_visa", Map.of(sku, 2));

        assertThat(awaitTerminal(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("OUT_OF_STOCK");
        assertThat(available(sku)).isEqualTo(1);
        assertThat(payments(orderId)).isEmpty();
    }

    @Test
    @Order(3)
    void cardDeclinedReleasesStock() {
        String sku = newSku(10);

        UUID orderId = placeOrder("pm_card_chargeDeclined", Map.of(sku, 4));

        assertThat(awaitTerminal(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("CARD_DECLINED");
        assertThat(available(sku)).isEqualTo(10);
        assertThat(reserved(sku)).isZero();
        assertThat(payments(orderId)).containsExactly("AUTHORIZE:FAILED");
    }

    @Test
    @Order(4)
    void captureFailureVoidsPaymentAndReleasesStock() {
        String sku = newSku(10);

        UUID orderId = placeOrder("pm_mock_captureFails", Map.of(sku, 4));

        assertThat(awaitTerminal(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("CAPTURE_DECLINED");
        assertThat(available(sku)).isEqualTo(10);
        assertThat(reserved(sku)).isZero();
        assertThat(payments(orderId)).containsExactlyInAnyOrder("AUTHORIZE:SUCCEEDED", "CAPTURE:FAILED", "VOID:SUCCEEDED");
    }

    @Test
    @Order(5)
    void retriedPostWithSameKeyPlacesOneOrder() {
        String sku = newSku(10);
        String key = UUID.randomUUID().toString();

        JsonNode first = placeOrder(key, "pm_card_visa", Map.of(sku, 1));
        JsonNode retry = placeOrder(key, "pm_card_visa", Map.of(sku, 1));

        UUID orderId = UUID.fromString(first.get("orderId").asText());
        assertThat(retry.get("orderId").asText()).isEqualTo(orderId.toString());
        assertThat(awaitTerminal(orderId)).isEqualTo("COMPLETED");
        await().atMost(Duration.ofSeconds(30)).until(() -> reserved(sku) == 0);
        assertThat(available(sku)).isEqualTo(9);
    }

    /**
     * Payment is down while an order is placed, so the saga stalls awaiting authorization. Order is
     * then stopped too. After both restart, the saga resumes from saga_instances and Kafka offsets
     * and completes (CLAUDE.md rules 8 and 9).
     */
    @Test
    @Order(6)
    void sagaResumesAfterPaymentOutageAndOrderRestart() {
        String sku = newSku(10);
        E2eStack.stopService("payment");
        try {
            UUID orderId = placeOrder("pm_card_visa", Map.of(sku, 2));
            await().atMost(Duration.ofMinutes(1)).until(() -> sagaState(orderId).equals("INVENTORY_RESERVED"));

            E2eStack.stopService("order");
            E2eStack.startService("payment");
            E2eStack.startService("order");

            assertThat(awaitTerminal(orderId)).isEqualTo("COMPLETED");
            await().atMost(Duration.ofSeconds(30)).until(() -> reserved(sku) == 0);
            assertThat(available(sku)).isEqualTo(8);
            assertThat(payments(orderId)).containsExactlyInAnyOrder("AUTHORIZE:SUCCEEDED", "CAPTURE:SUCCEEDED");
        } finally {
            // Leave the stack whole for later tests even if this one failed midway.
            E2eStack.ensureRunning("payment");
            E2eStack.ensureRunning("order");
        }
    }
}
