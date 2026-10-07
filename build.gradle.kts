plugins {
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
}

allprojects {
    group = "com.nexus"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    plugins.withType<JavaPlugin> {
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(21)
            }
        }

        tasks.withType<JavaCompile>().configureEach {
            options.encoding = "UTF-8"
            options.compilerArgs.add("-parameters")
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            // Integration tests register the real Debezium connector config used by `make connectors`.
            val connectorTemplate = rootProject.file("deploy/connect/outbox-connector.json")
            inputs.file(connectorTemplate)
            systemProperty("nexus.connectorTemplate", connectorTemplate.absolutePath)
        }
    }
}
