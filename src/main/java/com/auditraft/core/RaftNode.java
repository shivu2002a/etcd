package com.auditraft.core;

import com.auditraft.grpc.AppendRequest;
import com.auditraft.grpc.AppendResponse;
import com.auditraft.grpc.VoteRequest;
import com.auditraft.grpc.VoteResponse;
import com.auditraft.grpc.LogEntry;
import java.util.*;

import com.auditraft.rpc.RaftPeerClient;
import io.grpc.stub.StreamObserver;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RaftNode {
    public enum State { FOLLOWER, CANDIDATE, LEADER }

    private final String nodeId;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<RaftPeerClient> peers = new CopyOnWriteArrayList<>();

    // RocksDB Engine
    private RocksDB db;
    private final String dbDir;
    static { RocksDB.loadLibrary(); }

    private long currentTerm = 0;
    private String votedFor = null;
    private volatile State state = State.FOLLOWER;
    private String knownLeader = null;
    private long commitIndex = 0;

    private long lastHeartbeatTime;
    private long currentElectionTimeout;
    private final Random random = new Random();
    
    private final ScheduledExecutorService timerExecutor = Executors.newScheduledThreadPool(2);
    private static final int MIN_ELECTION_TIMEOUT = 2000;
    private static final int MAX_ELECTION_TIMEOUT = 4000;
    private static final int HEARTBEAT_INTERVAL = 500;

    private long lastLogIndex = 0;
    private final List<LogEntry> log = new ArrayList<>();
    
    public RaftNode(String nodeId) {
        this.nodeId = nodeId;
        
        this.dbDir = "./raft-data/" + nodeId;
        try {
            Files.createDirectories(Paths.get(dbDir));
            Options options = new Options().setCreateIfMissing(true);
            this.db = RocksDB.open(options, dbDir);
        } catch (Exception e) {
            throw new RuntimeException("CRITICAL: Failed to initialize RocksDB for " + nodeId, e);
        }

        resetTimer();
        startElectionTimer();
    }

    private void resetTimer() {
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.currentElectionTimeout = MIN_ELECTION_TIMEOUT + random.nextInt(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT);
    }

    public void addPeer(RaftPeerClient peer) { this.peers.add(peer); }
    public String getNodeId() { return nodeId; }
    public State getState() { return state; }
    
    public String getKnownLeader() {
        lock.readLock().lock();
        try { return knownLeader; } finally { lock.readLock().unlock(); }
    }

    public long getCurrentTerm() {
        lock.readLock().lock();
        try { return currentTerm; } finally { lock.readLock().unlock(); }
    }

    public String getVotedFor() {
        lock.readLock().lock();
        try { return votedFor; } finally { lock.readLock().unlock(); }
    }

    public void putData(String key, String value) {
        try { db.put(key.getBytes(), value.getBytes()); } 
        catch (RocksDBException e) {}
    }

    public String getData(String key) {
        try {
            byte[] val = db.get(key.getBytes());
            return val != null ? new String(val) : null;
        } catch (RocksDBException e) { return null; }
    }

    // --- State Transitions (Called by Receivers) ---

    public void stepDown(long newTerm) {
        lock.writeLock().lock();
        try {
            this.currentTerm = newTerm;
            this.state = State.FOLLOWER;
            this.votedFor = null;
            resetTimer();
        } finally { lock.writeLock().unlock(); }
    }

    public void grantVote(String candidateId) {
        lock.writeLock().lock();
        try {
            this.votedFor = candidateId;
            resetTimer();
        } finally { lock.writeLock().unlock(); }
    }

    public void resetHeartbeat(AppendRequest request) {
        lock.writeLock().lock();
        try {
            // Use getters to extract info from the Protobuf request object
            if (request.getTerm() >= this.currentTerm) {
                this.knownLeader = request.getLeaderId();
                this.currentTerm = request.getTerm();
                this.state = State.FOLLOWER;
                resetTimer();

                // 📦 Phase 4: Process incoming Log Entries
                for (LogEntry entry : request.getEntriesList()) {
                    // Only apply entries we haven't committed yet
                    if (entry.getIndex() > this.commitIndex) {
                        putData(entry.getKey(), entry.getValue());
                        this.commitIndex = entry.getIndex();
                        System.out.println("📥 [" + nodeId + "] Replicated & Committed: " + entry.getKey());
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // --- Timers & Broadcasting ---

    private void startElectionTimer() {
        timerExecutor.scheduleAtFixedRate(() -> {
            try {
                lock.writeLock().lock();
                try {
                    if (state == State.LEADER) return;
                    if (System.currentTimeMillis() - lastHeartbeatTime > currentElectionTimeout) {
                        startElection();
                    }
                } finally { lock.writeLock().unlock(); }
            } catch (Exception e) { e.printStackTrace(); }
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        long electionTerm;
        int majorityNeeded = (peers.size() + 1) / 2 + 1;
        AtomicInteger votesReceived = new AtomicInteger(1); 

        lock.writeLock().lock();
        try {
            this.state = State.CANDIDATE;
            this.currentTerm++;
            this.votedFor = this.nodeId;
            resetTimer(); 
            electionTerm = this.currentTerm;
            System.out.println("\n🗳️ [" + nodeId + "] Election timeout! Started election for term " + currentTerm);
        } finally { lock.writeLock().unlock(); }

        VoteRequest request = VoteRequest.newBuilder()
                .setTerm(electionTerm).setCandidateId(this.nodeId)
                .setLastLogIndex(0).setLastLogTerm(0).build();

        for (RaftPeerClient peer : peers) {
            peer.sendVoteRequest(request, new StreamObserver<VoteResponse>() {
                @Override public void onNext(VoteResponse response) {
                    if (response.getTerm() > electionTerm) {
                        stepDown(response.getTerm());
                        return;
                    }
                    if (response.getVoteGranted() && state == State.CANDIDATE) {
                        if (votesReceived.incrementAndGet() >= majorityNeeded) {
                            transitionToLeader();
                        }
                    }
                }
                @Override public void onError(Throwable t) {} // 🔇 Intentionally blank to stop log spam
                @Override public void onCompleted() {}
            });
        }
    }

    private void transitionToLeader() {
        lock.writeLock().lock();
        try {
            if (this.state == State.LEADER) return; 
            this.state = State.LEADER;
            this.knownLeader = this.nodeId;
            System.out.println("👑 [" + nodeId + "] Won election! Now LEADER for term " + currentTerm);
            timerExecutor.scheduleAtFixedRate(this::broadcastHeartbeats, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
        } finally { lock.writeLock().unlock(); }
    }

    /**
     * Entry point for new data. 
     * The Leader appends to its local log but does NOT write to RocksDB yet.
     */
    public boolean propose(String key, String value) {
        lock.writeLock().lock();
        try {
            if (this.state != State.LEADER) return false;

            lastLogIndex++;
            LogEntry entry = LogEntry.newBuilder()
                    .setTerm(currentTerm)
                    .setIndex(lastLogIndex)
                    .setKey(key)
                    .setValue(value)
                    .build();
            
            log.add(entry);
            System.out.println("📝 [" + nodeId + "] Appended to log at index " + lastLogIndex);
            
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void broadcastHeartbeats() {
        lock.readLock().lock();
        long currentTermSnapshot;
        long commitIndexSnapshot;
        try {
            if (this.state != State.LEADER) return;
            currentTermSnapshot = this.currentTerm;
            commitIndexSnapshot = this.commitIndex;
        } finally { lock.readLock().unlock(); }

        for (RaftPeerClient peer : peers) {
            // Check if we have entries to send to this peer
            // (Simplified: Sending the whole log for now. In Phase 5 we'll track nextIndex per peer)
            AppendRequest request = AppendRequest.newBuilder()
                    .setTerm(currentTermSnapshot)
                    .setLeaderId(this.nodeId)
                    .setLeaderCommit(commitIndexSnapshot)
                    .addAllEntries(log) 
                    .build();

            peer.sendAppendEntries(request, new StreamObserver<AppendResponse>() {
                @Override public void onNext(AppendResponse response) {
                    if (response.getTerm() > currentTermSnapshot) stepDown(response.getTerm());
                    // Logic for updating commitIndex goes here next!
                }
                @Override public void onError(Throwable t) {}
                @Override public void onCompleted() {}
            });
        }
    }
}