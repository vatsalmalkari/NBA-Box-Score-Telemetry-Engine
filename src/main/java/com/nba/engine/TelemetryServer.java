package com.nba.engine;

import com.nba.engine.grpc.PeerClient;
import com.nba.engine.grpc.TelemetryServiceImpl;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TelemetryServer {

    public static void main(String[] args)
            throws IOException, InterruptedException {

        NodeConfig config = NodeConfig.fromEnvironment();

        Server server = ServerBuilder
                .forPort(50051)
                .addService(new TelemetryServiceImpl(config.nodeId()))
                .build()
                .start();

        System.out.println(
                config.nodeId() + " started on port 50051"
        );

        List<PeerClient> peerClients = new ArrayList<>();

        for (String peer : config.peers()) {
            peerClients.add(new PeerClient(peer));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();

            for (PeerClient client : peerClients) {
                client.close();
            }
        }));

        // Try to contact each peer.
        for (int i = 0; i < peerClients.size(); i++) {
            PeerClient client = peerClients.get(i);
            String peer = config.peers().get(i);

            boolean connected = false;

            for (int attempt = 1; attempt <= 10; attempt++) {
                try {
                    client.ping(config.nodeId());
                    connected = true;
                    break;
                } catch (Exception e) {
                    System.out.println(
                            config.nodeId() + " cannot reach " + peer
                                    + " (attempt " + attempt + "/10)"
                    );

                    Thread.sleep(1000);
                }
            }

            if (!connected) {
                System.out.println(
                        config.nodeId() + " could not reach " + peer
                );
            }
        }

        server.awaitTermination();
    }
}