package com.nba.engine.raft;

import com.nba.engine.grpc.AppendRequest;
import com.nba.engine.grpc.AppendResponse;
import com.nba.engine.grpc.PeerClient;
import com.nba.engine.grpc.VoteRequest;
import com.nba.engine.grpc.VoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class OneLeaderPerTermTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final Map<String, RaftNode> byId = new HashMap<>();
    private final Map<String, List<InProcessPeer>> peersById = new HashMap<>();
    private final Map<String, Path> statePaths = new HashMap<>();
    private final Set<String> stopped = new HashSet<>();

    private static final List<String> IDS = List.of("node-1", "node-2", "node-3");

    static class InProcessPeer extends PeerClient {
        private final String targetId;
        private volatile RaftNode target;
        private volatile boolean down = false;

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

        void setDown(boolean down) {
            this.down = down;
        }

        private RaftNode node() {
            if (down) {
                throw new RuntimeException("UNAVAILABLE: peer " + targetId + " is down");
            }
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
        buildCluster();
        startAll();

        Map<Long, Set<String>> seen = new HashMap<>();
        sampleFor(10_000, seen);

        assertFalse(seen.isEmpty(), "no leader was elected within 10 seconds");
        assertNoTermHasTwoLeaders(seen);
    }

    @Test
    void newLeaderElectedAfterLeaderStopsAndOldLeaderRejoinsQuietly() throws Exception {
        buildCluster();
        startAll();

        Map<Long, Set<String>> seen = new HashMap<>();

        // Wait for an initial leader.
        String oldLeader = awaitLeader(seen, 5_000);
        assertNotNull(oldLeader, "no initial leader within 5 seconds");
        long oldTerm = byId.get(oldLeader).getCurrentTerm();

        // Stop the leader: its timers stop, and every peer pointing at it fails.
        stopped.add(oldLeader);
        byId.get(oldLeader).shutdown();
        for (String id : IDS) {
            if (id.equals(oldLeader)) continue;
            for (InProcessPeer peer : peersById.get(id)) {
                if (peer.targetId().equals(oldLeader)) {
                    peer.setDown(true);
                }
            }
        }

        // A survivor must take over in a higher term.
        String newLeader = awaitLeader(seen, 3_000);
        assertNotNull(newLeader, "no new leader within 3 seconds of stopping " + oldLeader);
        assertNotEquals(oldLeader, newLeader, "stopped node cannot be the new leader");
        long newTerm = byId.get(newLeader).getCurrentTerm();
        assertTrue(newTerm > oldTerm, "new leader term " + newTerm + " should exceed old term " + oldTerm);

        // Restart the old leader from its saved state file, reconnect its peers.
        RaftNode restarted = new RaftNode(oldLeader, new ArrayList<PeerClient>(peersById.get(oldLeader)), new RaftStateStore(statePaths.get(oldLeader).toString()));
        nodes.add(restarted);
        byId.put(oldLeader, restarted);
        stopped.remove(oldLeader);

        for (String id : IDS) {
            if (id.equals(oldLeader)) continue;
            for (InProcessPeer peer : peersById.get(id)) {
                if (peer.targetId().equals(oldLeader)) {
                    peer.connect(restarted);
                    peer.setDown(false);
                }
            }
        }
        restarted.start();

        // Watch for 3 seconds. The rejoin must not disturb the new leader.
        sampleFor(3_000, seen);

        assertEquals(RaftRole.LEADER, byId.get(newLeader).getRole(), newLeader + " lost leadership after " + oldLeader + " rejoined");
        assertEquals(newTerm, byId.get(newLeader).getCurrentTerm(), "term changed after " + oldLeader + " rejoined");
        assertNotEquals(RaftRole.LEADER, restarted.getRole(), oldLeader + " became leader again after rejoining");
        assertNoTermHasTwoLeaders(seen);
    }

    private void buildCluster() throws IOException {
        for (String id : IDS) {
            List<InProcessPeer> peers = new ArrayList<>();
            for (String other : IDS) {
                if (!other.equals(id)) {
                    peers.add(new InProcessPeer(other));
                }
            }
            peersById.put(id, peers);
        }

        for (String id : IDS) {
            Path path = tempDir.resolve(id + ".properties");
            statePaths.put(id, path);

            RaftNode node = new RaftNode(id, new ArrayList<PeerClient>(peersById.get(id)), new RaftStateStore(path.toString()));
            nodes.add(node);
            byId.put(id, node);
        }

        for (String id : IDS) {
            for (InProcessPeer peer : peersById.get(id)) {
                peer.connect(byId.get(peer.targetId()));
            }
        }
    }

    private void startAll() {
        for (RaftNode node : nodes) {
            node.start();
        }
    }

    /** Samples live nodes every 10ms for the given duration. */
    private void sampleFor(long durationMs, Map<Long, Set<String>> seen)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + durationMs;
        while (System.currentTimeMillis() < deadline) {
            record(seen);
            Thread.sleep(10);
        }
    }

    /** Polls live nodes until one is leader, recording every observation. */
    private String awaitLeader(Map<Long, Set<String>> seen, long timeoutMs)
throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            record(seen);
            for (String id : IDS) {
                if (stopped.contains(id)){
                    continue;
                }
                if (byId.get(id).leaderTermIfLeader().isPresent()) {
                    return id;
                }
            }
            Thread.sleep(10);
        }
        return null;
    }

    private void record(Map<Long, Set<String>> seen) {
        for (String id : IDS) {
            if (stopped.contains(id)){
                continue;
            }
            byId.get(id).leaderTermIfLeader().ifPresent(term -> seen.computeIfAbsent(term, t -> new TreeSet<>()).add(id));
        }
    }

    private void assertNoTermHasTwoLeaders(Map<Long, Set<String>> seen) {
        for (Map.Entry<Long, Set<String>> entry : seen.entrySet()) {
            assertTrue(entry.getValue().size() <= 1, "term " + entry.getKey() + " had multiple leaders: " + entry.getValue());
        }
    }
}