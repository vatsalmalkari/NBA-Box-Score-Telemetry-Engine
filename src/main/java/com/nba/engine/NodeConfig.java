package com.nba.engine;

import java.util.Arrays;
import java.util.List;

public record NodeConfig(String nodeId, List<String> peers) {

    public static NodeConfig fromEnvironment() {
        String nodeId = System.getenv("NODE_ID");
        String peers = System.getenv("PEERS");

        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalStateException("NODE_ID is not configured");
        }

        if (peers == null || peers.isBlank()) {
            throw new IllegalStateException("PEERS is not configured");
        }

        List<String> peerList = Arrays.stream(peers.split(","))
                .map(String::trim)
                .filter(peer -> !peer.isEmpty())
                .toList();

        return new NodeConfig(nodeId, peerList);
    }
}