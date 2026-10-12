plugins {
    java
}

dependencies {
    testImplementation(platform(libs.spring.boot.bom))
    testImplementation(testFixtures(project(":libs:messaging")))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-json")
    testRuntimeOnly("org.postgresql:postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val services = listOf("order", "inventory", "payment", "fraud", "fulfillment", "reconciliation")

tasks.test {
    // Slow (each service runs as a container): only with `-Pe2e`, i.e. `make e2e` / `make phase1-check`.
    onlyIf { project.hasProperty("e2e") }
    outputs.upToDateWhen { false }

    for (service in services) {
        dependsOn(":services:$service:bootJar")
        val jar = project(":services:$service").layout.buildDirectory.file("libs/$service-${project.version}.jar")
        systemProperty("nexus.jar.$service", jar.get().asFile.absolutePath)
    }
    systemProperty("nexus.repoRoot", rootProject.projectDir.absolutePath)
    systemProperty("nexus.logDir", layout.buildDirectory.dir("e2e-logs").get().asFile.absolutePath)
    // Done check: number of orders, and where to write the results JSON (unset = don't write).
    systemProperty("nexus.doneCheck.orders", providers.gradleProperty("doneCheckOrders").getOrElse("200"))
    // Phase 2 check: target orders/sec, seconds a killed service stays down, seconds between kills.
    systemProperty("nexus.doneCheck.rate", providers.gradleProperty("doneCheckRate").getOrElse("5"))
    systemProperty("nexus.doneCheck.downtimeSeconds", providers.gradleProperty("doneCheckDowntime").getOrElse("10"))
    systemProperty("nexus.doneCheck.settleSeconds", providers.gradleProperty("doneCheckSettle").getOrElse("20"))
    // S6: inconsistencies injected per finding type.
    systemProperty("nexus.s6.injectionsPerType", providers.gradleProperty("s6Injections").getOrElse("5"))
    providers.gradleProperty("resultsDir").orNull?.let { systemProperty("nexus.resultsDir", it) }
    providers.gradleProperty("e2eTests").orNull?.let { filter.includeTestsMatching(it) }

    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
    }
}
