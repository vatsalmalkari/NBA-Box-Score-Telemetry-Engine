package com.nba.engine.grpc;

import com.nba.engine.raft.RaftNode;
import io.grpc.stub.StreamObserver;

public class TelemetryServiceImpl
        extends RaftTelemetryServiceGrpc.RaftTelemetryServiceImplBase {

    private final String nodeId;
    private final RaftNode raftNode;

    public TelemetryServiceImpl(String nodeId, RaftNode raftNode) {
        this.nodeId = nodeId;
        this.raftNode = raftNode;
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {

        System.out.println(nodeId + " received ping from " + request.getSenderId());

        PingResponse response = PingResponse.newBuilder().setReceiverId(nodeId).setSuccess(true).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {

        VoteResponse response = raftNode.handleVoteRequest(request);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {

        AppendResponse response = raftNode.handleAppendEntries(request);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override 
    public void preVote(VoteRequest request, StreamObserver<VoteResponse> observer) {
        observer.onNext(raftNode.handlePreVote(request));
        observer.onCompleted();
    }
}