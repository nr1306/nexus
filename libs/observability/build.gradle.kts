plugins {
    `java-library`
}

// Metrics + tracing shared by every Java service (SPEC §9). Metrics: the Prometheus registry behind
// /actuator/prometheus. Tracing: Micrometer Tracing on the OpenTelemetry bridge, W3C
// traceparent through HTTP, gRPC, Kafka headers and the outbox. Export over OTLP is off until a collector
// exists (TRACING_EXPORT_ENABLED / OTEL_EXPORTER_OTLP_ENDPOINT).
dependencies {
    api(platform(libs.spring.boot.bom))
    // Optional: services with an outbox get its traceparent wired; Fraud has none.
    compileOnly(project(":libs:messaging"))
    api("org.springframework.boot:spring-boot-starter-actuator")
    api("io.micrometer:micrometer-tracing-bridge-otel")
    api("io.micrometer:micrometer-registry-prometheus")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp")

    testImplementation(project(":libs:messaging"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
