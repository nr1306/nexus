package com.nexus.messaging.testing;

import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

/**
 * Registers the real outbox connector template ({@code deploy/connect/outbox-connector.json}) in tests.
 * The template path comes from the {@code nexus.connectorTemplate} system property set by Gradle.
 */
public final class OutboxConnectors {

    private OutboxConnectors() {
    }

    /**
     * Registers {@code <service>-outbox} and waits until its replication slot is streaming.
     * The service's outbox table must already exist.
     */
    public static void register(GenericContainer<?> connect, JdbcTemplate serviceDb, String service) {
        String config;
        try {
            config = Files.readString(Path.of(System.getProperty("nexus.connectorTemplate")))
                    .replace("__SERVICE__", service);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read connector template", e);
        }

        String url = "http://" + connect.getHost() + ":" + connect.getMappedPort(8083)
                + "/connectors/" + service + "-outbox/config";
        HttpResponse<String> response;
        try {
            response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(url))
                            .header("Content-Type", "application/json")
                            .PUT(HttpRequest.BodyPublishers.ofString(config))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("Cannot reach Kafka Connect", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        if (response.statusCode() != 200 && response.statusCode() != 201) {
            throw new IllegalStateException("Connector registration failed: " + response.statusCode() + " " + response.body());
        }

        // Rows written after the slot is active arrive via the WAL stream.
        await().atMost(Duration.ofMinutes(2)).until(() -> serviceDb.queryForObject(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = ? AND active",
                Integer.class, service + "_outbox") == 1);
    }
}
