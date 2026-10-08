package com.nexus.fraud.grpc;

import com.nexus.fraud.config.FraudProperties;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

/**
 * Runs the gRPC server alongside the Spring context: started after beans are ready, stopped
 * gracefully (in-flight calls get 5 s) before the context closes. Also serves the standard gRPC health
 * service and reflection (for grpcurl).
 */
@Component
public class GrpcServer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GrpcServer.class);

    private final FraudGrpcService fraudGrpcService;
    private final FraudProperties properties;
    private final HealthStatusManager health = new HealthStatusManager();
    private volatile Server server;

    public GrpcServer(FraudGrpcService fraudGrpcService, FraudProperties properties) {
        this.fraudGrpcService = fraudGrpcService;
        this.properties = properties;
    }

    @Override
    public void start() {
        try {
            server = NettyServerBuilder.forPort(properties.grpcPort())
                    .addService(fraudGrpcService)
                    .addService(health.getHealthService())
                    .addService(ProtoReflectionServiceV1.newInstance())
                    .build()
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot start gRPC server on port " + properties.grpcPort(), e);
        }
        log.info("gRPC server listening on port {}", server.getPort());
    }

    @Override
    public void stop() {
        Server s = server;
        if (s == null) {
            return;
        }
        health.enterTerminalState();
        s.shutdown();
        try {
            if (!s.awaitTermination(5, TimeUnit.SECONDS)) {
                s.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            s.shutdownNow();
        }
        server = null;
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }

    /** The bound port (useful when configured as 0). */
    public int port() {
        return server.getPort();
    }
}
