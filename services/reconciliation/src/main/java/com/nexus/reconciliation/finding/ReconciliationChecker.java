package com.nexus.reconciliation.finding;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.messaging.envelope.EventEnvelope;
import com.nexus.messaging.outbox.OutboxWriter;
import com.nexus.reconciliation.config.ReconciliationProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One reconciliation pass (ADR 0009): for every invariant, record new violations among settled
 * orders (sending the repair command, if any, in the same transaction), resolve findings whose
 * invariant holds again, and re-send repairs that haven't taken effect.
 */
@Component
public class ReconciliationChecker {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationChecker.class);
    private static final String AGGREGATE_TYPE = "reconciliation";
    private static final int SCHEMA_VERSION = 1;
    private static final int BATCH = 500;
    /** Saga id for a repair when the projection never saw one (cf. Inventory's ReservationExpired). */
    private static final UUID UNKNOWN_SAGA = new UUID(0, 0);

    private final FindingRepository findings;
    private final OutboxWriter outbox;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final ReconciliationProperties properties;
    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final AtomicLong openFindings = new AtomicLong();

    public ReconciliationChecker(FindingRepository findings, OutboxWriter outbox, ObjectMapper objectMapper,
                                 TransactionTemplate transactionTemplate, ReconciliationProperties properties,
                                 MeterRegistry meterRegistry, Clock clock) {
        this.findings = findings;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.clock = clock;
        meterRegistry.gauge("reconciliation_open_findings", openFindings);
    }

    /** @return number of new findings */
    public int check() {
        Instant now = clock.instant();
        Instant settledBefore = now.minus(properties.settleWindow());
        Instant stuckBefore = now.minus(properties.stuckAfter());
        int detected = 0;
        for (FindingType type : FindingType.values()) {
            for (FindingRepository.Target target : findings.findNewViolations(type, settledBefore, stuckBefore, BATCH)) {
                if (Boolean.TRUE.equals(transactionTemplate.execute(status -> detect(target, type, now)))) {
                    detected++;
                }
            }
            int resolved = transactionTemplate.execute(status -> findings.resolveFixed(type, now, stuckBefore));
            if (resolved > 0) {
                meterRegistry.counter("reconciliation_findings_resolved_total", "type", type.name()).increment(resolved);
                log.info("Resolved {} {} finding(s)", resolved, type);
            }
            if (type.repairable()) {
                for (FindingRepository.Target target : findings.findRepairsToRetry(type,
                        now.minus(properties.repairRetryAfter()), properties.maxRepairAttempts(), BATCH)) {
                    transactionTemplate.executeWithoutResult(status -> repair(target, type, now));
                }
            }
        }
        openFindings.set(findings.countOpen());
        return detected;
    }

    private boolean detect(FindingRepository.Target target, FindingType type, Instant now) {
        if (!findings.insert(target, type, now)) {
            return false;
        }
        meterRegistry.counter("reconciliation_findings_total", "type", type.name()).increment();
        try (var orderId = MDC.putCloseable("orderId", target.orderId().toString())) {
            log.warn("Finding {}{}", type, type.repairable() ? "; repairing with " + type.repairCommand : "");
            if (type.repairable()) {
                repair(target, type, now);
            }
        }
        return true;
    }

    private void repair(FindingRepository.Target target, FindingType type, Instant now) {
        UUID sagaId = target.sagaId() != null ? target.sagaId() : UNKNOWN_SAGA;
        var payload = objectMapper.createObjectNode().put("reason", "RECONCILIATION_" + type.name());
        EventEnvelope command = EventEnvelope.create(type.repairCommand, SCHEMA_VERSION, target.orderId(), sagaId, payload, clock);
        outbox.append(type.repairTopic, AGGREGATE_TYPE, command);
        findings.recordRepairSent(target.orderId(), type, now);
        meterRegistry.counter("reconciliation_repairs_sent_total", "command", type.repairCommand).increment();
    }
}
