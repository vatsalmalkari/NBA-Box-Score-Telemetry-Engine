package com.nba.engine.raft;

import com.nba.engine.grpc.AppendRequest;
import com.nba.engine.grpc.AppendResponse;
import com.nba.engine.grpc.PeerClient;
import com.nba.engine.grpc.VoteRequest;
import com.nba.engine.grpc.VoteResponse;
import java.util.Optional;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RaftNode {

    private static final int LEADER_LEASE_MS = 400;

    private final String nodeId;
    private final List<PeerClient> peers;
    private final RaftStateStore stateStore;
    private final RaftLog raftLog;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final ExecutorService rpcExecutor = Executors.newVirtualThreadPerTaskExecutor();

    private final Map<PeerClient, AtomicBoolean> heartbeatInFlight;

    private final int clusterSize;
    private final int majority;

    private RaftRole role = RaftRole.FOLLOWER;

    private long currentTerm;
    private String votedFor;
    private String leaderId;

    private ScheduledFuture<?> electionTask;
    private ScheduledFuture<?> heartbeatTask;

    // Peers that granted a vote in the current real election.
    // A Set makes counting idempotent a duplicate reply can't raise the count.
    private final Set<String> votesGranted = new HashSet<>();

    // Pre-vote state. electionRound identifies one election attempt, so replies from an older attempt are ignored.
    private long electionRound = 0;
    private int preVotesReceived = 0;
    private long commitIndex = 0;
    private long lastApplied = 0;

    private long lastLeaderContactMillis = 0;

    public RaftNode(String nodeId, List<PeerClient> peers, RaftStateStore stateStore, RaftLog raftLog) 
    throws IOException {

        this.nodeId = nodeId;
        this.peers = peers;
        this.stateStore = stateStore;
        this.raftLog = raftLog;

        this.currentTerm = stateStore.loadTerm();
        this.votedFor = stateStore.loadVotedFor();

        this.clusterSize = peers.size() + 1;
        this.majority = clusterSize / 2 + 1;

        this.heartbeatInFlight = new HashMap<>();
        for (PeerClient peer : peers) {
            heartbeatInFlight.put(peer, new AtomicBoolean(false));
        }

        System.out.println(nodeId + " recovered term=" + currentTerm + ", votedFor=" + votedFor);
    }

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

        electionTask = scheduler.schedule( this::startElection, timeoutMs, TimeUnit.MILLISECONDS);
    }

    // Pre-vote phase

    private void startElection() {
        final long round;
        final long proposedTerm;

        synchronized (this) {
            if (role == RaftRole.LEADER) {
                return;
            }

            // The timer fired just as a leader's heartbeat arrived.
            // Don't start an election while the leader is still active.
            if (heardFromLeaderRecently()) {
                resetElectionTimer();
                return;
            }

            round = ++electionRound;
            proposedTerm = currentTerm + 1;
            preVotesReceived = 1; // vote for self
            resetElectionTimer();

            if (preVotesReceived >= majority) {
                beginRealElection();
                return;
            }
        }

        for (PeerClient peer : peers) {
            rpcExecutor.execute(() -> preVoteFromPeer(peer, round, proposedTerm));
        }
    }

    private void preVoteFromPeer(PeerClient peer, long round, long proposedTerm) {
        VoteRequest request = buildVoteRequest(proposedTerm);

        try {
            VoteResponse response = peer.preVote(request);

            synchronized (this) {
                if (response.getTerm() > currentTerm) {
                    stepDown(response.getTerm());
                    return;
                }

                // Ignore replies from an older round, a round that already
                // became a real election, or a term we've moved past.
                if (round != electionRound || role == RaftRole.LEADER || proposedTerm != currentTerm + 1 || !response.getVoteGranted()) {
                    return;
                }

                preVotesReceived++;

                if (preVotesReceived >= majority) {
                    beginRealElection();
                }
            }
        } catch (Exception e) {
            System.out.println(nodeId + " pre-vote failed: " + e.getMessage());
        }
    }

    // Caller must hold the RaftNode monitor.
    private void beginRealElection() {
        if (role == RaftRole.LEADER) {
            return;
        }

        // Invalidate any remaining pre-vote replies for this round.
        electionRound++;

        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        leaderId = null;
        votesGranted.clear();
        votesGranted.add(nodeId);
        persistState();

        final long electionTerm = currentTerm;

        System.out.println(nodeId + " became CANDIDATE for term " + currentTerm);

        if (votesGranted.size() >= majority) {
            becomeLeader();
            return;
        }

        for (PeerClient peer : peers) {
            rpcExecutor.execute(() -> requestVoteFromPeer(peer, electionTerm));
        }
    }

    // ---------- Real vote phase ----------

    private void requestVoteFromPeer(PeerClient peer, long electionTerm) {
        VoteRequest request = buildVoteRequest(electionTerm);

        try {
            VoteResponse response = peer.requestVote(request);

            synchronized (this) {
                if (response.getTerm() > currentTerm) {
                    stepDown(response.getTerm());
                    return;
                }

                if (role != RaftRole.CANDIDATE || currentTerm != electionTerm) {
                    return;
                }

                // add() returns false if this peer already counted, so a
                // duplicate or late reply can't push the count over the line.
                if (response.getVoteGranted() && votesGranted.add(peer.getPeerAddress())) {
                    System.out.println(nodeId + " received vote for term " + electionTerm + " (" + votesGranted.size() + "/" + majority + ")");

                    if (votesGranted.size() >= majority) {
                        becomeLeader();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(nodeId + " could not obtain vote from peer: " + e.getMessage());
        }
    }

    public synchronized VoteResponse handlePreVote(VoteRequest request) {
        // A pre-vote never changes local state. Deny it if we are the leader,
        // or if we heard from a leader recently. Only a proposed term above ours can win.
        boolean grant = role != RaftRole.LEADER && request.getTerm() > currentTerm && !heardFromLeaderRecently() && isCandidateLogUpToDate(request);

        return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(grant).build();
    }

    public synchronized VoteResponse handleVoteRequest(VoteRequest request) {
        // Reject candidates from older terms outright.
        if (request.getTerm() < currentTerm) {
            return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
        }

        // Leader stickiness: while we hear from a leader, refuse to let a
        // higher term disrupt the cluster. Do not update our term here.
        if (heardFromLeaderRecently() && request.getTerm() > currentTerm) {
            return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
        }

        if (request.getTerm() > currentTerm) {
            updateTerm(request.getTerm());
        }

        if (!isCandidateLogUpToDate(request)) {
            return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
        }

        // The log is not implemented yet, so both nodes have empty logs.
        boolean candidateLogIsUpToDate = isCandidateLogUpToDate(request);

        boolean canVote = votedFor == null || votedFor.equals(request.getCandidateId());

        boolean grantVote = canVote && candidateLogIsUpToDate;

        if (grantVote) {
            votedFor = request.getCandidateId();
            persistState();

            role = RaftRole.FOLLOWER;
            leaderId = null;

            resetElectionTimer();

            System.out.println(nodeId + " voted for " + request.getCandidateId() + " in term " + currentTerm);
        }

        return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(grantVote).build();
    }

    public synchronized AppendResponse handleAppendEntries(AppendRequest request) {
        if (request.getTerm() < currentTerm) {
            return AppendResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
        }

        // A valid leader for this term or a newer one. Record contact first,
        // so the lease covers this heartbeat.
        lastLeaderContactMillis = System.currentTimeMillis();

        if (request.getTerm() > currentTerm) {
            updateTerm(request.getTerm());
        }

        if (role != RaftRole.FOLLOWER) {
            System.out.println(nodeId + " demoted by leader " + request.getLeaderId() + " in term " + request.getTerm());
        }

        role = RaftRole.FOLLOWER;
        leaderId = request.getLeaderId();

        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }

        // Reset the election timer on every valid leader message,
        // before any log checks.
        resetElectionTimer();

        if (request.getPrevLogIndex() != 0 || !request.getEntriesList().isEmpty()) {
            return AppendResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
        }

        return AppendResponse.newBuilder().setTerm(currentTerm).setSuccess(true).build();
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

        System.out.println(nodeId + " became LEADER for term " + currentTerm);

        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, 75, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeats() {
        final long term;

        synchronized (this) {
            if (role != RaftRole.LEADER) {
                return;
            }
            term = currentTerm;
        }

        AppendRequest request = AppendRequest.newBuilder().setTerm(term).setLeaderId(nodeId).setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build();

        for (PeerClient peer : peers) {
            AtomicBoolean busy = heartbeatInFlight.get(peer);
            if (!busy.compareAndSet(false, true)) {
                continue; // previous heartbeat to this peer still pending
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
                    System.out.println(nodeId + " heartbeat failed: " + e.getMessage());
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

        System.out.println(nodeId + " stepped down; current term=" + currentTerm);
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
            throw new IllegalStateException("Could not persist Raft state", e);
        }
    }

    private long lastLogIndex() {
        return raftLog.lastIndex();
    }

    private long lastLogTerm() {
        try {
                return raftLog.lastTerm();
        }   
        catch (IOException e) {
                throw new IllegalStateException("Failed to read last log term", e);
        }
    }

    private VoteRequest buildVoteRequest(long term) {
        return VoteRequest.newBuilder().setTerm(term).setCandidateId(nodeId).setLastLogIndex(lastLogIndex()).setLastLogTerm(lastLogTerm()).build();
    }

    private boolean isCandidateLogUpToDate(VoteRequest request) {
        long localLastTerm = lastLogTerm();
        long localLastIndex = lastLogIndex();

        if (request.getLastLogTerm() != localLastTerm) {
            return request.getLastLogTerm() > localLastTerm;
        }

        return request.getLastLogIndex() >= localLastIndex;
    }

    private boolean heardFromLeaderRecently() {
        return System.currentTimeMillis() - lastLeaderContactMillis < LEADER_LEASE_MS;
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

    public synchronized Optional<Long> leaderTermIfLeader() {
        return role == RaftRole.LEADER ? Optional.of(currentTerm) : Optional.empty();
    }

    public void shutdown() {
        heartbeatTask = null;
        scheduler.shutdownNow();
        rpcExecutor.shutdownNow();
    }
}