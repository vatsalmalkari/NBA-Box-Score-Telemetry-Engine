package com.nba.engine.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class PeerClient implements AutoCloseable {

    private final String peerAddress;
    private final ManagedChannel channel;

    private final RaftTelemetryServiceGrpc
            .RaftTelemetryServiceBlockingStub blockingStub;

    public PeerClient(String peerAddress) {
        this.peerAddress = peerAddress;

        this.channel = ManagedChannelBuilder
                .forTarget(peerAddress)
                .usePlaintext()
                .build();

        this.blockingStub =
                RaftTelemetryServiceGrpc.newBlockingStub(channel);
    }

    public void ping(String senderId) {
        PingRequest request = PingRequest.newBuilder()
                .setSenderId(senderId)
                .build();

        PingResponse response = blockingStub
                .withDeadlineAfter(3, TimeUnit.SECONDS)
                .ping(request);

        System.out.println(
                senderId + " pinged " + peerAddress
                        + " | receiver=" + response.getReceiverId()
                        + " | success=" + response.getSuccess()
        );
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