package com.nba.engine.raft;

import com.nba.engine.grpc.PeerClient;
import com.nba.engine.grpc.VoteRequest;
import com.nba.engine.grpc.VoteResponse;
import com.nba.engine.grpc.AppendRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.nba.engine.grpc.AppendResponse;
import java.util.stream.Collectors; 
import java.util.Map;


public class RaftNode {

    private final String nodeId;
    private final List<PeerClient> peers;
    private final RaftStateStore stateStore;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final ExecutorService rpcExecutor = Executors.newVirtualThreadPerTaskExecutor();

    private RaftRole role = RaftRole.FOLLOWER;

    private long currentTerm;
    private String votedFor;
    private String leaderId;

    private ScheduledFuture<?> electionTask;
    private ScheduledFuture<?> heartbeatTask;

    private final int clusterSize;
    private final int majority;

    private int votesReceived;

    public RaftNode(
            String nodeId,
            List<PeerClient> peers,
            RaftStateStore stateStore
    ) throws IOException {

        this.nodeId = nodeId;
        this.peers = peers;
        this.stateStore = stateStore;

        this.currentTerm = stateStore.loadTerm();
        this.votedFor = stateStore.loadVotedFor();
        this.heartbeatInFlight = peers.stream().collect(Collectors.toMap(p -> p, p -> new AtomicBoolean(false)));

        this.clusterSize = peers.size() + 1;
        this.majority = clusterSize / 2 + 1;

        System.out.println(
                nodeId + " recovered term=" + currentTerm
                        + ", votedFor=" + votedFor
        );
    }

        private final Map<PeerClient, AtomicBoolean> heartbeatInFlight;

    public synchronized void start() {
        resetElectionTimer();
    }

    private synchronized void resetElectionTimer() {
        if (role == RaftRole.LEADER) {
            return;
        }

        if (electionTask != null) {
            electionTask.cancel(false);
        }

        int timeoutMs = ThreadLocalRandom.current().nextInt(500, 801);

        electionTask = scheduler.schedule(
                this::startElection,
                timeoutMs,
                TimeUnit.MILLISECONDS
        );
    }

    private void startElection() {
        final long electionTerm;

        synchronized (this) {
            if (role == RaftRole.LEADER) {
                return;
            }

            role = RaftRole.CANDIDATE;
            currentTerm++;
            votedFor = nodeId;
            leaderId = null;
            votesReceived = 1;

            persistState();

            electionTerm = currentTerm;

            System.out.println(
                    nodeId + " became CANDIDATE for term " + currentTerm
            );

            resetElectionTimer();

            if (votesReceived >= majority) {
                becomeLeader();
                return;
            }
        }

        for (PeerClient peer : peers) {
            rpcExecutor.execute(() -> requestVoteFromPeer(
                    peer, electionTerm
            ));
        }
    }

    private void requestVoteFromPeer(
            PeerClient peer,
            long electionTerm
    ) {
        VoteRequest request = VoteRequest.newBuilder()
                .setTerm(electionTerm)
                .setCandidateId(nodeId)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        try {
            VoteResponse response = peer.requestVote(request);

            synchronized (this) {
                if (response.getTerm() > currentTerm) {
                    stepDown(response.getTerm());
                    return;
                }

                if (role != RaftRole.CANDIDATE
                        || currentTerm != electionTerm) {
                    return;
                }

                if (response.getVoteGranted()) {
                    votesReceived++;

                    System.out.println(
                            nodeId + " received vote for term "
                                    + electionTerm + " ("
                                    + votesReceived + "/" + majority + ")"
                    );

                    if (votesReceived >= majority) {
                        becomeLeader();
                    }
                }
            }

        } catch (Exception e) {
            System.out.println(
                    nodeId + " could not obtain vote from peer: "
                            + e.getMessage()
            );
        }
    }

    public synchronized VoteResponse handleVoteRequest(
            VoteRequest request
    ) {
        if (request.getTerm() < currentTerm) {
            return VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }

        if (request.getTerm() > currentTerm) {
            updateTerm(request.getTerm());
        }

        // The log is not implemented yet, so both nodes should
        // currently have an empty log.
        boolean candidateLogIsUpToDate =
                request.getLastLogIndex() == 0
                        && request.getLastLogTerm() == 0;

        boolean canVote =
                votedFor == null
                        || votedFor.equals(request.getCandidateId());

        boolean grantVote = canVote && candidateLogIsUpToDate;

        if (grantVote) {
            votedFor = request.getCandidateId();
            persistState();

            role = RaftRole.FOLLOWER;
            leaderId = null;

            resetElectionTimer();

            System.out.println(
                    nodeId + " voted for " + request.getCandidateId()
                            + " in term " + currentTerm
            );
        }

        return VoteResponse.newBuilder()
                .setTerm(currentTerm)
                .setVoteGranted(grantVote)
                .build();
    }

    public synchronized AppendResponse handleAppendEntries(AppendRequest request) {
        if (request.getTerm() < currentTerm) {
            return AppendResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();
        }

        if (request.getTerm() > currentTerm) {
            updateTerm(request.getTerm());
        }

        role = RaftRole.FOLLOWER;
        leaderId = request.getLeaderId();

        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }

        if (request.getPrevLogIndex() != 0
                || !request.getEntriesList().isEmpty()) {
            return AppendResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();
        }

        resetElectionTimer();

        return AppendResponse.newBuilder()
                .setTerm(currentTerm)
                .setSuccess(true)
                .build();
    }

    private void becomeLeader() {
        if (role != RaftRole.CANDIDATE) {
            return;
        }

        role = RaftRole.LEADER;
        leaderId = nodeId;

        if (electionTask != null) {
            electionTask.cancel(false);
        }

        System.out.println(
                nodeId + " became LEADER for term " + currentTerm
        );

        heartbeatTask = scheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                0,
                75,
                TimeUnit.MILLISECONDS
        );
    }

private void sendHeartbeats() {
    final long term;

    synchronized (this) {
        if (role != RaftRole.LEADER) {
            return;
        }

        term = currentTerm;
    }

    AppendRequest request = AppendRequest.newBuilder()
            .setTerm(term)
            .setLeaderId(nodeId)
            .setPrevLogIndex(0)
            .setPrevLogTerm(0)
            .setLeaderCommit(0)
            .build();

    for (PeerClient peer : peers) {
        AtomicBoolean busy = heartbeatInFlight.get(peer);
        if (!busy.compareAndSet(false, true)) {
            continue;
        }
        rpcExecutor.execute(() -> {
            try {
                AppendResponse response = peer.appendEntries(request);
                synchronized (this) {
                    if (response.getTerm() > currentTerm) {
                        stepDown(response.getTerm());
                    }
                }
            } catch (Exception e) {
                System.out.println(
                        nodeId + " heartbeat failed: " + e.getMessage());
            } finally {
                busy.set(false);
            }
        });
    }
}

    private synchronized void stepDown(long newTerm) {
        updateTerm(newTerm);
        role = RaftRole.FOLLOWER;
        leaderId = null;

        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }

        resetElectionTimer();

        System.out.println(
                nodeId + " stepped down; current term=" + currentTerm
        );
    }

        private void updateTerm(long newTerm) {
        currentTerm = newTerm;
        votedFor = null;
        role = RaftRole.FOLLOWER;
        leaderId = null;

        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }

        persistState();
        }

    private void persistState() {
        try {
            stateStore.save(currentTerm, votedFor);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could not persist Raft state", e
            );
        }
    }

    public synchronized String getNodeId() {
        return nodeId;
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized RaftRole getRole() {
        return role;
    }

    public synchronized String getLeaderId() {
        return leaderId;
    }
}