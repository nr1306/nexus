package com.nexus.e2e;

import com.nexus.messaging.testing.NexusContainers;
import com.nexus.messaging.testing.OutboxConnectors;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The whole system for end-to-end tests: one Postgres (a database per service), Kafka,
 * Debezium Connect with the real outbox connectors, and each service's bootJar in its own JRE
 * container. Started once per test JVM. Service logs go to {@code tests/e2e/build/e2e-logs/}.
 */
final class E2eStack {

    static final String JRE_IMAGE = "eclipse-temurin:21-jre";
    static final List<String> SERVICES = List.of("inventory", "payment", "fraud", "fulfillment", "order");
    /** Services that publish through an outbox and need a Debezium connector. */
    static final List<String> OUTBOX_SERVICES = List.of("inventory", "payment", "fulfillment", "order");

    private static final Network NETWORK = Network.newNetwork();
    static final PostgreSQLContainer<?> POSTGRES = NexusContainers.postgres(NETWORK, "order_db");
    static final KafkaContainer KAFKA = NexusContainers.kafka(NETWORK);
    static final GenericContainer<?> CONNECT = NexusContainers.connect(NETWORK).dependsOn(KAFKA, POSTGRES);

    private static final Map<String, GenericContainer<?>> RUNNING = new LinkedHashMap<>();
    private static final Map<String, JdbcTemplate> DATABASES = new LinkedHashMap<>();

    private static boolean started;

    private E2eStack() {
    }

    static synchronized void start() {
        if (started) {
            return;
        }
        Startables.deepStart(POSTGRES, KAFKA, CONNECT).join();
        db("order").execute("CREATE DATABASE inventory_db");
        db("order").execute("CREATE DATABASE payment_db");
        db("order").execute("CREATE DATABASE fraud_db");
        db("order").execute("CREATE DATABASE fulfillment_db");

        // Services run their Flyway migrations on startup; connectors need the outbox tables.
        SERVICES.forEach(E2eStack::startService);
        OUTBOX_SERVICES.forEach(s -> OutboxConnectors.register(CONNECT, db(s), s));
        started = true;
    }

    static synchronized void startService(String service) {
        GenericContainer<?> container = new GenericContainer<>(JRE_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(service)
                .withCopyFileToContainer(MountableFile.forHostPath(System.getProperty("nexus.jar." + service)), "/app.jar")
                .withCommand("java", "-jar", "/app.jar")
                .withEnv(Map.of(
                        "DB_URL", "jdbc:postgresql://postgres:5432/" + service + "_db",
                        "DB_USER", NexusContainers.DB_USER,
                        "DB_PASSWORD", NexusContainers.DB_PASSWORD,
                        "KAFKA_BOOTSTRAP_SERVERS", "kafka:19092",
                        "FRAUD_GRPC_TARGET", "fraud:9090",
                        "GRPC_PORT", "9090",
                        "SERVER_PORT", "8080"))
                .withExposedPorts(8080)
                .withLogConsumer(frame -> appendLog(service, frame))
                .waitingFor(Wait.forHttp("/actuator/health").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        container.start();
        RUNNING.put(service, container);
    }

    /** Stops the service's container (its database and Kafka offsets survive). */
    static synchronized void stopService(String service) {
        GenericContainer<?> container = RUNNING.remove(service);
        if (container != null) {
            container.stop();
        }
    }

    static synchronized void ensureRunning(String service) {
        if (!RUNNING.containsKey(service)) {
            startService(service);
        }
    }

    static String orderApi() {
        GenericContainer<?> order = RUNNING.get("order");
        return "http://" + order.getHost() + ":" + order.getMappedPort(8080);
    }

    static synchronized JdbcTemplate db(String service) {
        return DATABASES.computeIfAbsent(service, s -> {
            String url = POSTGRES.getJdbcUrl().replace("/order_db", "/" + s + "_db");
            return new JdbcTemplate(new DriverManagerDataSource(url, POSTGRES.getUsername(), POSTGRES.getPassword()));
        });
    }

    private static void appendLog(String service, OutputFrame frame) {
        try {
            Path dir = Path.of(System.getProperty("nexus.logDir"));
            Files.createDirectories(dir);
            Files.writeString(dir.resolve(service + ".log"), frame.getUtf8String(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
