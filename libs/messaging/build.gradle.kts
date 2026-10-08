plugins {
    `java-library`
    `java-test-fixtures`
}

dependencies {
    api(platform(libs.spring.boot.bom))

    api("org.springframework.boot:spring-boot-starter-jdbc")
    api("org.springframework.boot:spring-boot-starter-json")
    api("org.springframework.kafka:spring-kafka")
    api("io.micrometer:micrometer-core")
    // DLQ admin endpoint; only active in web applications.
    compileOnly("org.springframework.boot:spring-boot-starter-web")

    // Shared Testcontainers setup for every service's integration tests.
    testFixturesApi(platform(libs.spring.boot.bom))
    testFixturesApi("org.testcontainers:junit-jupiter")
    testFixturesApi("org.testcontainers:postgresql")
    testFixturesApi("org.testcontainers:kafka")
    testFixturesApi("org.awaitility:awaitility")
    testFixturesApi("org.springframework:spring-jdbc")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.flywaydb:flyway-core")
    testImplementation("org.flywaydb:flyway-database-postgresql")
    testRuntimeOnly("org.postgresql:postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

