package com.nexus.fraud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param grpcPort          gRPC listen port; 0 picks a free port (tests)
 * @param maxAmountCents    orders above this total are rejected (AMOUNT_LIMIT)
 * @param velocityMaxOrders a customer's order is rejected if they already placed this many within the window (VELOCITY)
 * @param velocityWindow    look-back window for the velocity rule
 */
@ConfigurationProperties("nexus.fraud")
public record FraudProperties(
        @DefaultValue("9090") int grpcPort,
        @DefaultValue("500000") long maxAmountCents,
        @DefaultValue("5") int velocityMaxOrders,
        @DefaultValue("PT10M") Duration velocityWindow) {
}
