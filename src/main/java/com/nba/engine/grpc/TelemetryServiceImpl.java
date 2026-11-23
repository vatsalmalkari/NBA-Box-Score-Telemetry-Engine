package com.nba.engine.grpc;

import io.grpc.stub.StreamObserver;

public class TelemetryServiceImpl
        extends RaftTelemetryServiceGrpc.RaftTelemetryServiceImplBase {

    private final String nodeId;

    public TelemetryServiceImpl(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void ping(
            PingRequest request,
            StreamObserver<PingResponse> responseObserver) {

        System.out.println(
                nodeId + " received ping from " + request.getSenderId()
        );

        PingResponse response = PingResponse.newBuilder()
                .setReceiverId(nodeId)
                .setSuccess(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}