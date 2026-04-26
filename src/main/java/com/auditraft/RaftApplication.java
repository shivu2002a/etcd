package com.auditraft;

import java.io.IOException;

import com.auditraft.core.RaftNode;
import com.auditraft.rpc.AuditConfigClientServiceImpl;
import com.auditraft.rpc.RaftInternalServiceImpl;
import com.auditraft.rpc.RaftPeerClient;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

public class RaftApplication {

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: java -jar app.jar <nodeId> <port> [peerHost:peerPort...]");
            System.exit(1);
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);

        // 1. Initialize the Raft State Machine
        RaftNode raftNode = new RaftNode(nodeId);

        // 2. Start the gRPC Server to RECEIVE network calls
        Server server = ServerBuilder.forPort(port)
                .addService(new RaftInternalServiceImpl(raftNode))
                .addService(new AuditConfigClientServiceImpl(raftNode))
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();

        System.out.println("🚀 [" + nodeId + "] Listening on port " + port);

        // 3. Connect to PEERS
        // We wait a second to ensure all manual startup commands have time to run
        Thread.sleep(2000); 
        for (int i = 2; i < args.length; i++) {
            String[] peerInfo = args[i].split(":");
            String peerHost = peerInfo[0];
            int peerPort = Integer.parseInt(peerInfo[1]);
            String peerId = "node-" + peerPort; 

            RaftPeerClient peerClient = new RaftPeerClient(peerId, peerHost, peerPort);
            raftNode.addPeer(peerClient);
            System.out.println("🔗 [" + nodeId + "] Registered peer: " + peerHost + ":" + peerPort);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));

        // 4. Start the Raft election timer AFTER peers are registered
        raftNode.start();
        System.out.println("✅ [" + nodeId + "] Raft node started with " + (args.length - 2) + " peers");

        server.awaitTermination();
    }
}