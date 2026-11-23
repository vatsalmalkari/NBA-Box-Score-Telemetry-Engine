package com.nba.engine;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class TelemetryServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder
                .forPort(50051)
                .build()
                .start();

        System.out.println("Telemetry server started on port 50051");

        server.awaitTermination();
    }
}