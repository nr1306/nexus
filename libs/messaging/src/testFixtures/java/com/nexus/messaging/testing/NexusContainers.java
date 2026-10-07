package com.nexus.messaging.testing;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Map;

/**
 * Container definitions matching {@code deploy/compose/docker-compose.yml}. Keep the image tags in sync.
 */
public final class NexusContainers {

    public static final String POSTGRES_IMAGE = "postgres:16";
    public static final String KAFKA_IMAGE = "apache/kafka:4.1.2";
    public static final String CONNECT_IMAGE = "quay.io/debezium/connect:3.7.0.Final";

    public static final String DB_USER = "nexus";
    public static final String DB_PASSWORD = "nexus";

    private NexusContainers() {
    }

    /** Postgres reachable as {@code postgres:5432} on the network, with logical decoding for Debezium. */
    public static PostgreSQLContainer<?> postgres(Network network, String databaseName) {
        return new PostgreSQLContainer<>(POSTGRES_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName(databaseName)
                .withUsername(DB_USER)
                .withPassword(DB_PASSWORD)
                .withCommand("postgres", "-c", "wal_level=logical");
    }

    /** Kafka reachable as {@code kafka:19092} on the network and via {@code getBootstrapServers()} from the host. */
    public static KafkaContainer kafka(Network network) {
        return new KafkaContainer(KAFKA_IMAGE)
                .withNetwork(network)
                .withListener("kafka:19092");
    }

    /** Debezium Connect worker configured like the local stack (env config provider, string converters). */
    public static GenericContainer<?> connect(Network network) {
        return new GenericContainer<>(CONNECT_IMAGE)
                .withNetwork(network)
                .withExposedPorts(8083)
                .withEnv(Map.ofEntries(
                        Map.entry("BOOTSTRAP_SERVERS", "kafka:19092"),
                        Map.entry("GROUP_ID", "nexus-connect-test"),
                        Map.entry("CONFIG_STORAGE_TOPIC", "_connect_configs"),
                        Map.entry("OFFSET_STORAGE_TOPIC", "_connect_offsets"),
                        Map.entry("STATUS_STORAGE_TOPIC", "_connect_status"),
                        Map.entry("CONFIG_STORAGE_REPLICATION_FACTOR", "1"),
                        Map.entry("OFFSET_STORAGE_REPLICATION_FACTOR", "1"),
                        Map.entry("STATUS_STORAGE_REPLICATION_FACTOR", "1"),
                        Map.entry("CONNECT_CONFIG_PROVIDERS", "env"),
                        Map.entry("CONNECT_CONFIG_PROVIDERS_ENV_CLASS",
                                "org.apache.kafka.common.config.provider.EnvVarConfigProvider"),
                        Map.entry("POSTGRES_USER", DB_USER),
                        Map.entry("POSTGRES_PASSWORD", DB_PASSWORD)))
                .waitingFor(Wait.forHttp("/connectors").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
    }
}
