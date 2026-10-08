plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":libs:messaging"))
    implementation(project(":contracts"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.resilience4j.circuitbreaker)
    implementation(libs.resilience4j.micrometer)

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation(testFixtures(project(":libs:messaging")))
    testImplementation(libs.grpc.inprocess)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
