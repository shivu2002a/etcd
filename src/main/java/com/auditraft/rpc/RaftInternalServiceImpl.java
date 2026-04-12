package com.auditraft.rpc;

import com.auditraft.core.RaftNode;
import com.auditraft.grpc.AppendRequest;
import com.auditraft.grpc.AppendResponse;
import com.auditraft.grpc.RaftInternalServiceGrpc;
import com.auditraft.grpc.VoteRequest;
import com.auditraft.grpc.VoteResponse;
import io.grpc.stub.StreamObserver;

public class RaftInternalServiceImpl extends RaftInternalServiceGrpc.RaftInternalServiceImplBase {

    private final RaftNode raftNode;

    public RaftInternalServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        System.out.println("Node " + raftNode.getNodeId() + " received VoteRequest from " 
                           + request.getCandidateId() + " for term " + request.getTerm());

        boolean voteGranted = false;
        long currentTerm = raftNode.getCurrentTerm();

        // 1. Reply false if candidate's term < currentTerm
        if (request.getTerm() < currentTerm) {
            voteGranted = false;
        } 
        // 2. If votedFor is null or candidateId, grant vote.
        else {
            // If the candidate has a strictly higher term, we must step down to follower
            if (request.getTerm() > currentTerm) {
                raftNode.stepDown(request.getTerm());
            }

            String votedFor = raftNode.getVotedFor();
            if (votedFor == null || votedFor.equals(request.getCandidateId())) {
                // TODO: Add log up-to-date check here once RocksDB is integrated (Phase 3)
                voteGranted = true;
                raftNode.grantVote(request.getCandidateId());
                System.out.println("Node " + raftNode.getNodeId() + " granted vote to " + request.getCandidateId());
            }
        }

        VoteResponse response = VoteResponse.newBuilder()
                .setTerm(raftNode.getCurrentTerm())
                .setVoteGranted(voteGranted)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {
        // This acts as our Heartbeat receiver (and eventually log receiver)
        boolean success = false;
        long currentTerm = raftNode.getCurrentTerm();

        if (request.getTerm() < currentTerm) {
            success = false; // Reject, leader is stale
        } else {
            // Recognize this node as the valid leader and reset our election timer
            raftNode.resetHeartbeat(request.getTerm(), request.getLeaderId());
            success = true;
            
            // TODO: Actually append the entries to RocksDB WAL here (Phase 3)
        }

        AppendResponse response = AppendResponse.newBuilder()
                .setTerm(raftNode.getCurrentTerm())
                .setSuccess(success)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}