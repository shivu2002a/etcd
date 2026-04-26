package com.auditraft.rpc;

import com.auditraft.core.RaftNode;
import com.auditraft.grpc.AuditConfigClientServiceGrpc;
import com.auditraft.grpc.GetRequest;
import com.auditraft.grpc.GetResponse;
import com.auditraft.grpc.PutRequest;
import com.auditraft.grpc.PutResponse;

import io.grpc.stub.StreamObserver;

public class AuditConfigClientServiceImpl extends AuditConfigClientServiceGrpc.AuditConfigClientServiceImplBase {

    private final RaftNode raftNode;

    public AuditConfigClientServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void putConfig(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        // 1. MUST BE LEADER TO WRITE
        if (raftNode.getState() != RaftNode.State.LEADER) {
            System.out.println("❌ [" + raftNode.getNodeId() + "] Rejected PutConfig (I am a " + raftNode.getState() + "). Redirecting to " + raftNode.getKnownLeader());
            
            PutResponse response = PutResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderId(raftNode.getKnownLeader() != null ? raftNode.getKnownLeader() : "UNKNOWN")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        // 2. WE ARE THE LEADER — append to replicated log, response deferred until commit
        System.out.println("📝 [" + raftNode.getNodeId() + "] LEADER accepted PutConfig: " + request.getKey() + "=" + request.getValue());
        raftNode.appendEntry(request.getKey(), request.getValue(), responseObserver);
        // Response will be sent by RaftNode.applyCommittedEntries() after majority replication
    }

    @Override
    public void getConfig(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        // Technically, Raft allows stale reads from followers, but strict linearizability 
        // requires routing reads to the leader too. We will route to leader for safety.
        if (raftNode.getState() != RaftNode.State.LEADER) {
            GetResponse response = GetResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderId(raftNode.getKnownLeader() != null ? raftNode.getKnownLeader() : "UNKNOWN")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        String value = raftNode.getData(request.getKey());
        GetResponse response = GetResponse.newBuilder()
                .setSuccess(true)
                .setValue(value != null ? value : "")
                .setLeaderId(raftNode.getNodeId())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}