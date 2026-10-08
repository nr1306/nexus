package com.nexus.inventory.reservation;

import com.nexus.inventory.config.InventoryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.UUID;

/**
 * Releases holds past their TTL (SPEC.md §7: abandoned sagas can't lock stock forever), one order per
 * transaction. Safe on several pods: the conditional UPDATE in {@link ReservationRepository#expireHeld}
 * hands each row to exactly one of them.
 */
@Component
public class ReservationExpirySweeper {

    private static final Logger log = LoggerFactory.getLogger(ReservationExpirySweeper.class);

    private final ReservationRepository reservations;
    private final ReservationService reservationService;
    private final TransactionTemplate transactionTemplate;
    private final InventoryProperties properties;
    private final Clock clock;

    public ReservationExpirySweeper(ReservationRepository reservations, ReservationService reservationService,
                                    TransactionTemplate transactionTemplate, InventoryProperties properties, Clock clock) {
        this.reservations = reservations;
        this.reservationService = reservationService;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    /** @return number of orders whose holds were expired */
    public int sweep() {
        int expired = 0;
        for (UUID orderId : reservations.findExpiredOrders(clock.instant(), properties.expirySweepBatchSize())) {
            try (var ignored = MDC.putCloseable("orderId", orderId.toString())) {
                if (Boolean.TRUE.equals(transactionTemplate.execute(status -> reservationService.expire(orderId)))) {
                    expired++;
                }
            } catch (RuntimeException e) {
                log.error("Failed to expire reservation", e);
            }
        }
        return expired;
    }
}
