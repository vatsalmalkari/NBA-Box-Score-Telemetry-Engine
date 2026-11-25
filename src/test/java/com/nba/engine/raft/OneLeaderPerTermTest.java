package com.nba.engine.raft;

import com.nba.engine.grpc.AppendRequest;
import com.nba.engine.grpc.AppendResponse;
import com.nba.engine.grpc.PeerClient;
import com.nba.engine.grpc.VoteRequest;
import com.nba.engine.grpc.VoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class OneLeaderPerTermTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();

     // A peer that calls another in-process RaftNode directly.
     // The gRPC channel is created by the superclass but never used.
    static class InProcessPeer extends PeerClient {
        private final String targetId;
        private volatile RaftNode target;

        InProcessPeer(String targetId) {
            super(targetId + ":50051");
            this.targetId = targetId;
        }

        String targetId() {
            return targetId;
        }

        void connect(RaftNode node) {
            this.target = node;
        }

        private RaftNode node() {
            RaftNode n = target;
            if (n == null) {
                throw new IllegalStateException("peer " + targetId + " not wired");
            }
            return n;
        }

        @Override
        public VoteResponse requestVote(VoteRequest request) {
            return node().handleVoteRequest(request);
        }

        @Override
        public VoteResponse preVote(VoteRequest request) {
            return node().handlePreVote(request);
        }

        @Override
        public AppendResponse appendEntries(AppendRequest request) {
            return node().handleAppendEntries(request);
        }
    }

    @AfterEach
    void tearDown() {
        for (RaftNode node : nodes) {
            node.shutdown();
        }
    }

    @Test
    void atMostOneLeaderPerTermOverTenSeconds() throws Exception {
        List<String> ids = List.of("node-1", "node-2", "node-3");

        List<InProcessPeer> allPeers = new ArrayList<>();
        Map<String, List<PeerClient>> peersById = new HashMap<>();

        for (String id : ids) {
            List<PeerClient> peers = new ArrayList<>();
            for (String other : ids) {
                if (!other.equals(id)) {
                    InProcessPeer peer = new InProcessPeer(other);
                    allPeers.add(peer);
                    peers.add(peer);
                }
            }
            peersById.put(id, peers);
        }

        Map<String, RaftNode> byId = new HashMap<>();
        for (String id : ids) {
            RaftStateStore store = new RaftStateStore(tempDir.resolve(id + ".properties").toString());
            RaftNode node = new RaftNode(id, peersById.get(id), store);
            nodes.add(node);
            byId.put(id, node);
        }

        for (InProcessPeer peer : allPeers) {
            peer.connect(byId.get(peer.targetId()));
        }

        for (RaftNode node : nodes) {
            node.start();
        }

        Map<Long, Set<String>> leadersByTerm = new HashMap<>();
        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            for (RaftNode node : nodes) {
                node.leaderTermIfLeader().ifPresent(term -> leadersByTerm.computeIfAbsent(term, t -> new TreeSet<>()).add(node.getNodeId()));
            }
            Thread.sleep(10);
        }

        assertFalse(leadersByTerm.isEmpty(), "no leader was elected within 10 seconds");

        for (Map.Entry<Long, Set<String>> entry : leadersByTerm.entrySet()) {
            assertTrue(entry.getValue().size() <= 1, "term " + entry.getKey() + " had multiple leaders: " + entry.getValue());
        }
    }
}