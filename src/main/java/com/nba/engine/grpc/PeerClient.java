package com.nba.engine.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class PeerClient implements AutoCloseable {

    private final String peerAddress;
    private final ManagedChannel channel;

    private final RaftTelemetryServiceGrpc.RaftTelemetryServiceBlockingStub stub;

    public PeerClient(String peerAddress) {
        this.peerAddress = peerAddress;

        this.channel = ManagedChannelBuilder
                .forTarget(peerAddress)
                .usePlaintext()
                .build();

        this.stub = RaftTelemetryServiceGrpc
                .newBlockingStub(channel);
    }

    public VoteResponse requestVote(VoteRequest request) {
        return stub.withDeadlineAfter(1, TimeUnit.SECONDS)
                .requestVote(request);
    }

    public AppendResponse appendEntries(AppendRequest request) {
        return stub.withDeadlineAfter(1, TimeUnit.SECONDS)
                .appendEntries(request);
    }

    public PingResponse ping(String senderId) {
        PingRequest request = PingRequest.newBuilder()
                .setSenderId(senderId)
                .build();

        return stub.withDeadlineAfter(3, TimeUnit.SECONDS)
                .ping(request);
    }

    public String getPeerAddress() {
        return peerAddress;
    }

    @Override
    public void close() {
        channel.shutdown();

        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}