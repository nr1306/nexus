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
 * Real Order, Inventory, Payment, Fraud and Fulfillment services talking over Kafka (via Debezium) and gRPC.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SagaE2eIT extends E2eSupport {

    /** Seeded in Inventory's catalog and in Fulfillment's restricted_skus. */
    private static final String RESTRICTED_SKU = "SKU-HAZMAT-01";

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
        // Fulfillment dispatches on OrderCompleted and notifies the customer.
        await().atMost(Duration.ofSeconds(30)).until(() -> "SHIPPED".equals(shipmentStatus(orderId)));
        await().atMost(Duration.ofSeconds(30)).until(() -> notifications(orderId).size() == 2);
        assertThat(notifications(orderId)).containsExactlyInAnyOrder("ORDER_CONFIRMED", "ORDER_SHIPPED");
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
    void fraudRejectionVoidsPaymentAndReleasesStock() {
        String sku = newSku(10);

        UUID orderId = placeOrder("pm_card_radarBlock", Map.of(sku, 3));

        assertThat(awaitTerminal(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("FRAUD_REJECTED");
        assertThat(available(sku)).isEqualTo(10);
        assertThat(reserved(sku)).isZero();
        assertThat(payments(orderId)).containsExactlyInAnyOrder("AUTHORIZE:SUCCEEDED", "VOID:SUCCEEDED");
    }

    @Test
    @Order(5)
    void fulfillmentFailureRefundsPaymentAndReleasesStock() {
        String sku = newSku(10);

        UUID orderId = placeOrder("pm_card_visa", Map.of(sku, 2, RESTRICTED_SKU, 1));

        assertThat(awaitTerminal(orderId)).isEqualTo("CANCELLED");
        assertThat(failureReason(orderId)).isEqualTo("RESTRICTED_SKU");
        assertThat(available(sku)).isEqualTo(10);
        assertThat(reserved(sku)).isZero();
        // Captured, then refunded; the authorization void after a refund is a no-op.
        assertThat(payments(orderId)).containsExactlyInAnyOrder(
                "AUTHORIZE:SUCCEEDED", "CAPTURE:SUCCEEDED", "REFUND:SUCCEEDED", "VOID:SKIPPED");
        assertThat(shipmentStatus(orderId)).isEqualTo("REJECTED");
        await().atMost(Duration.ofSeconds(30)).until(() -> notifications(orderId).contains("ORDER_CANCELLED"));
        assertThat(notifications(orderId)).containsExactly("ORDER_CANCELLED");
    }

    @Test
    @Order(5)
    void sagaWaitsOutAShortFraudOutage() {
        String sku = newSku(10);
        E2eStack.stopService("fraud");
        try {
            UUID orderId = placeOrder("pm_card_visa", Map.of(sku, 1));
            await().atMost(Duration.ofMinutes(1)).until(() -> sagaState(orderId).equals("PAYMENT_AUTHORIZED"));

            E2eStack.startService("fraud");

            assertThat(awaitTerminal(orderId)).isEqualTo("COMPLETED");
            assertThat(payments(orderId)).containsExactlyInAnyOrder("AUTHORIZE:SUCCEEDED", "CAPTURE:SUCCEEDED");
        } finally {
            E2eStack.ensureRunning("fraud");
        }
    }

    @Test
    @Order(5)
    void retriedPostWithSameKeyPlacesOneOrder() {
        String sku = newSku(10);
        String key = UUID.randomUUID().toString();
        String customer = newCustomer();

        JsonNode first = placeOrder(key, customer, "pm_card_visa", Map.of(sku, 1));
        JsonNode retry = placeOrder(key, customer, "pm_card_visa", Map.of(sku, 1));

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
