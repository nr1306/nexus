plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "nexus"

include(
    "contracts",
    "libs:messaging",
    "libs:observability",
    "services:order",
    "services:inventory",
    "services:payment",
    "services:fraud",
    "services:fulfillment",
    "services:reconciliation",
    "tests:e2e",
)
