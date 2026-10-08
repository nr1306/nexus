plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "nexus"

include(
    "contracts",
    "libs:messaging",
    "services:order",
    "services:inventory",
    "services:payment",
    "services:fraud",
    "services:fulfillment",
    "tests:e2e",
)
