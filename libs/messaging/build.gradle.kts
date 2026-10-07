plugins {
    `java-library`
}

dependencies {
    api(platform(libs.spring.boot.bom))

    api("org.springframework.boot:spring-boot-starter-jdbc")
    api("org.springframework.boot:spring-boot-starter-json")
    api("org.springframework.kafka:spring-kafka")
    api("io.micrometer:micrometer-core")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.flywaydb:flyway-core")
    testImplementation("org.flywaydb:flyway-database-postgresql")
    testImplementation("org.awaitility:awaitility")
    testRuntimeOnly("org.postgresql:postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val connectorTemplate = rootProject.file("deploy/connect/outbox-connector.json")

tasks.test {
    // OutboxDebeziumIT registers the real connector config used by `make connectors`.
    inputs.file(connectorTemplate)
    systemProperty("nexus.connectorTemplate", connectorTemplate.absolutePath)
}
