package com.auditraft.rpc;

import com.auditraft.grpc.AppendRequest;
import com.auditraft.grpc.AppendResponse;
import com.auditraft.grpc.RaftInternalServiceGrpc;
import com.auditraft.grpc.VoteRequest;
import com.auditraft.grpc.VoteResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class RaftPeerClient {

    private final String peerId;
    private final ManagedChannel channel;
    private final RaftInternalServiceGrpc.RaftInternalServiceStub asyncStub;

    public RaftPeerClient(String peerId, String host, int port) {
        this.peerId = peerId;
        // Plaintext channel for internal cluster communication
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.asyncStub = RaftInternalServiceGrpc.newStub(channel);
    }

    public String getPeerId() {
        return peerId;
    }

    public void sendVoteRequest(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        asyncStub.requestVote(request, responseObserver);
    }

    public void sendAppendEntries(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {
        asyncStub.appendEntries(request, responseObserver);
    }

    public void shutdown() {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
    }
}