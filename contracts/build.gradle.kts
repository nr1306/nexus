import com.google.protobuf.gradle.id

plugins {
    `java-library`
    alias(libs.plugins.protobuf)
}

// Java stubs generated from contracts/proto (gRPC). JSON Schemas in contracts/events are documentation
// and are not compiled.
sourceSets {
    main {
        proto {
            srcDir("proto")
        }
    }
}

dependencies {
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.protobuf.java)
    compileOnly(libs.tomcat.annotations.api)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.asProvider().get()}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}
