package com.nba.engine;

import com.nba.engine.grpc.PeerClient;
import com.nba.engine.grpc.TelemetryServiceImpl;
import com.nba.engine.raft.RaftLog;
import com.nba.engine.raft.RaftNode;
import com.nba.engine.raft.RaftStateStore;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TelemetryServer {

    public static void main(String[] args)
            throws IOException, InterruptedException {

        NodeConfig config = NodeConfig.fromEnvironment();

        String stateFile = System.getenv().getOrDefault("RAFT_STATE_FILE", "/data/raft-state.properties");

        List<PeerClient> peerClients = new ArrayList<>();

        for (String peer : config.peers()) {
            peerClients.add(new PeerClient(peer));
        }

        RaftStateStore stateStore = new RaftStateStore(stateFile);
        String walDir = System.getenv().getOrDefault("WAL_DIR", "/data/wal");

        RaftLog raftLog = new RaftLog(Paths.get(walDir, "raft-log.wal"));

        RaftNode raftNode = new RaftNode(config.nodeId(), peerClients, stateStore, raftLog);

        Server server = ServerBuilder.forPort(50051).addService(new TelemetryServiceImpl(config.nodeId(),raftNode)).build().start();

        System.out.println(config.nodeId() + " started on port 50051");

        raftNode.start();
        
        raftNode.shutdown();

        try {
        raftLog.close();
        } catch (IOException e) {
        e.printStackTrace();
        }

        server.shutdown();

        for (PeerClient peer : peerClients) {
        peer.close();
        }
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                        System.out.println(config.nodeId() + " shutting down");
                        server.shutdown();
                                for (PeerClient peer : peerClients) {
                                        peer.close();
                                }
                        }
                )
        );

        server.awaitTermination();
    }
}