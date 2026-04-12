package com.auditraft.core;

import com.auditraft.grpc.AppendRequest;
import com.auditraft.grpc.AppendResponse;
import com.auditraft.grpc.VoteRequest;
import com.auditraft.grpc.VoteResponse;
import com.auditraft.rpc.RaftPeerClient;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RaftNode {

    private String knownLeader = null; // Tracks who the current leader is
    
    // --- Persistent Storage Engine ---
    private RocksDB db;
    private final String dbDir;

    static {
        // This links the native C++ RocksDB binaries to the JVM
        RocksDB.loadLibrary();
    }

    public enum State { FOLLOWER, CANDIDATE, LEADER }

    private final String nodeId;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<RaftPeerClient> peers = new CopyOnWriteArrayList<>();

    private long currentTerm = 0;
    private String votedFor = null;
    private volatile State state = State.FOLLOWER;
    private long commitIndex = 0;

    private long lastHeartbeatTime;
    private final Random random = new Random();
    
    // Timers
    private final ScheduledExecutorService timerExecutor = Executors.newScheduledThreadPool(2);
    private static final int MIN_ELECTION_TIMEOUT = 2000;
    private static final int MAX_ELECTION_TIMEOUT = 4000;
    private static final int HEARTBEAT_INTERVAL = 500;

    public RaftNode(String nodeId) {

        this.nodeId = nodeId;
        this.lastHeartbeatTime = System.currentTimeMillis();

        // Isolate each node's data in its own folder
        this.dbDir = "./raft-data/" + nodeId;
        try {
            Files.createDirectories(Paths.get(dbDir));
            Options options = new Options().setCreateIfMissing(true);
            this.db = RocksDB.open(options, dbDir);
            System.out.println("💾 [" + nodeId + "] RocksDB initialized at " + dbDir);
        } catch (Exception e) {
            throw new RuntimeException("CRITICAL: Failed to initialize RocksDB for " + nodeId, e);
        }
        startElectionTimer();
    }

    public void addPeer(RaftPeerClient peer) {
        this.peers.add(peer);
    }

    // --- State Accessors ---
    public String getNodeId() { return nodeId; }
    
    public long getCurrentTerm() {
        lock.readLock().lock();
        try { return currentTerm; } finally { lock.readLock().unlock(); }
    }

    public String getVotedFor() {
        lock.readLock().lock();
        try { return votedFor; } finally { lock.readLock().unlock(); }
    }

    public State getState() {
        return this.state;
    }

    public String getKnownLeader() {
        lock.readLock().lock();
        try { return knownLeader; } finally { lock.readLock().unlock(); }
    }

    public void putData(String key, String value) {
        try {
            // RocksDB operates exclusively on byte arrays for maximum performance
            db.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            System.err.println("❌ [" + nodeId + "] Failed to write to disk: " + e.getMessage());
        }
    }

    public String getData(String key) {
        try {
            byte[] valueBytes = db.get(key.getBytes());
            return valueBytes != null ? new String(valueBytes) : null;
        } catch (RocksDBException e) {
            System.err.println("❌ [" + nodeId + "] Failed to read from disk: " + e.getMessage());
            return null;
        }
    }

    // --- Receiver Handlers (Called by gRPC Server) ---
    public void stepDown(long newTerm) {
        lock.writeLock().lock();
        try {
            this.currentTerm = newTerm;
            this.state = State.FOLLOWER;
            this.votedFor = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void grantVote(String candidateId) {
        lock.writeLock().lock();
        try {
            this.votedFor = candidateId;
            this.lastHeartbeatTime = System.currentTimeMillis();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void resetHeartbeat(long leaderTerm, String leaderId) {
        lock.writeLock().lock();
        try {
            if (leaderTerm >= this.currentTerm) {
                this.knownLeader = leaderId;
                this.currentTerm = leaderTerm;
                this.state = State.FOLLOWER;
                this.lastHeartbeatTime = System.currentTimeMillis();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // --- Background Timers & Active Logic ---
    private void startElectionTimer() {
        timerExecutor.scheduleAtFixedRate(() -> {
            try {
                lock.writeLock().lock();
                try {
                    if (state == State.LEADER) return;
                    long timeout = MIN_ELECTION_TIMEOUT + random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT);
                    if (System.currentTimeMillis() - lastHeartbeatTime > timeout) {
                        startElection();
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            } catch (Exception e) { e.printStackTrace(); }
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        long electionTerm;
        int majorityNeeded = (peers.size() + 1) / 2 + 1;
        AtomicInteger votesReceived = new AtomicInteger(1); // Vote for self

        this.state = State.CANDIDATE;
        this.currentTerm++;
        this.votedFor = this.nodeId;
        this.lastHeartbeatTime = System.currentTimeMillis();
        electionTerm = this.currentTerm;
        
        System.out.println("\n🗳️ [" + nodeId + "] Election timeout! Started election for term " + currentTerm);

        VoteRequest request = VoteRequest.newBuilder()
                .setTerm(electionTerm)
                .setCandidateId(this.nodeId)
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        for (RaftPeerClient peer : peers) {
            peer.sendVoteRequest(request, new StreamObserver<VoteResponse>() {
                @Override
                public void onNext(VoteResponse response) {
                    if (response.getTerm() > electionTerm) {
                        stepDown(response.getTerm());
                        return;
                    }
                    if (response.getVoteGranted() && state == State.CANDIDATE) {
                        int votes = votesReceived.incrementAndGet();
                        if (votes >= majorityNeeded) {
                            transitionToLeader();
                        }
                    }
                }
                @Override public void onError(Throwable t) {}
                @Override public void onCompleted() {}
            });
        }
    }

    private void transitionToLeader() {
        lock.writeLock().lock();
        try {
            if (this.state == State.LEADER) return; // Prevent double-trigger
            this.knownLeader = this.nodeId;
            this.state = State.LEADER;
            System.out.println("👑 [" + nodeId + "] Won election! Now LEADER for term " + currentTerm);
            
            // Start heartbeating immediately
            timerExecutor.scheduleAtFixedRate(this::broadcastHeartbeats, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void broadcastHeartbeats() {
        long currentTermSnapshot;
        lock.readLock().lock();
        try {
            if (this.state != State.LEADER) return;
            currentTermSnapshot = this.currentTerm;
        } finally { lock.readLock().unlock(); }

        AppendRequest heartbeat = AppendRequest.newBuilder()
                .setTerm(currentTermSnapshot)
                .setLeaderId(this.nodeId)
                .setLeaderCommit(commitIndex)
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .build();

        for (RaftPeerClient peer : peers) {
            peer.sendAppendEntries(heartbeat, new StreamObserver<AppendResponse>() {
                @Override
                public void onNext(AppendResponse response) {
                    if (response.getTerm() > currentTermSnapshot) {
                        stepDown(response.getTerm());
                    }
                }
                @Override public void onError(Throwable t) {}
                @Override public void onCompleted() {}
            });
        }
    }
}