package com.nexus.messaging.retry;

import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.kafka.retrytopic.SuffixingRetryTopicNamesProviderFactory.SuffixingRetryTopicNamesProvider;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Retry and DLQ topic names (ADR 0004). The consumer group is part of the name so two groups reading the
 * same topic never share retries or dead letters:
 * <pre>
 *   inventory.events.order-saga.retry-1s / -5s / -30s
 *   inventory.events.order-saga.dlq
 * </pre>
 */
public final class RetryTopicNaming {

    private static final Pattern DELAY_MS = Pattern.compile("-(\\d+)$");

    private RetryTopicNaming() {
    }

    public static String retrySuffix(String group) {
        return "." + group + ".retry";
    }

    public static String dlqSuffix(String group) {
        return "." + group + ".dlq";
    }

    public static String dlqTopic(String sourceTopic, String group) {
        return sourceTopic + dlqSuffix(group);
    }

    /** Spring names retry topics {@code <suffix>-<delayMs>}; this renders the delay as {@code 1s}, {@code 250ms}, ... */
    public static RetryTopicNamesProviderFactory namesProviderFactory() {
        return properties -> new SuffixingRetryTopicNamesProvider(properties) {
            @Override
            public String getTopicName(String topic) {
                String name = super.getTopicName(topic);
                return properties.isRetryTopic() ? humanizeDelay(name) : name;
            }
        };
    }

    static String humanizeDelay(String topicName) {
        Matcher m = DELAY_MS.matcher(topicName);
        if (!m.find()) {
            return topicName;
        }
        long ms = Long.parseLong(m.group(1));
        String delay = ms % 1000 == 0 ? (ms / 1000) + "s" : ms + "ms";
        return topicName.substring(0, m.start()) + "-" + delay;
    }
}
