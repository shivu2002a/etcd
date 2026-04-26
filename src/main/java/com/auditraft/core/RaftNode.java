package com.auditraft.core;

import com.auditraft.grpc.AppendRequest;
import com.auditraft.grpc.AppendResponse;
import com.auditraft.grpc.PutResponse;
import com.auditraft.grpc.VoteRequest;
import com.auditraft.grpc.VoteResponse;

import com.auditraft.rpc.RaftPeerClient;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
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

    // Track the heartbeat task so we can cancel it on step-down
    private java.util.concurrent.ScheduledFuture<?> heartbeatTask = null;

    // Replicated log (uses LogEntryData, not proto LogEntry)
    private final ReplicatedLog log = new ReplicatedLog();

    // --- Raft State Persistence ---
    private static final String RAFT_TERM_KEY = "__raft_term__";
    private static final String RAFT_VOTED_FOR_KEY = "__raft_votedFor__";
    private static final String RAFT_LOG_PREFIX = "__raft_log_";

    // Leader volatile state (reinitialized on election)
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // State machine application tracking
    private long lastApplied = 0;

    // Pending client requests: logIndex → StreamObserver<PutResponse>
    private final Map<Long, StreamObserver<PutResponse>> pendingRequests = new ConcurrentHashMap<>();

    // --- Snapshot fields (Task 5.1) ---
    private long snapshotThreshold = 1000;
    private long lastSnapshotIndex = 0;

    // --- Cluster configuration (Task 5.1 / 6.1) ---
    private ClusterConfiguration clusterConfig;

    // --- Membership change fields (Task 6.1) ---
    private boolean pendingConfigChange = false;
    private StreamObserver<?> pendingConfigObserver = null;

    // --- Catch-up state (Task 6.3) ---
    private CatchUpState activeCatchUp = null;

    // --- Raft State Persistence Helpers ---

    private void persistTerm() {
        try { db.put(RAFT_TERM_KEY.getBytes(), String.valueOf(currentTerm).getBytes()); }
        catch (RocksDBException e) { System.err.println("Failed to persist term: " + e.getMessage()); }
    }

    private void persistVotedFor() {
        try {
            String val = votedFor != null ? votedFor : "";
            db.put(RAFT_VOTED_FOR_KEY.getBytes(), val.getBytes());
        } catch (RocksDBException e) { System.err.println("Failed to persist votedFor: " + e.getMessage()); }
    }

    private void persistLogEntry(long index, LogEntryData entry) {
        try {
            String key = String.format("%s%010d__", RAFT_LOG_PREFIX, index);
            String val = entry.getTerm() + ":" + entry.getKey() + ":" + entry.getValue();
            db.put(key.getBytes(), val.getBytes());
        } catch (RocksDBException e) { System.err.println("Failed to persist log entry " + index + ": " + e.getMessage()); }
    }

    private void deleteLogEntry(long index) {
        try {
            String key = String.format("%s%010d__", RAFT_LOG_PREFIX, index);
            db.delete(key.getBytes());
        } catch (RocksDBException e) { System.err.println("Failed to delete log entry " + index + ": " + e.getMessage()); }
    }

    // --- Snapshot Metadata Persistence (Task 5.1) ---

    private void persistSnapshotMetadata(long lastIndex, long lastTerm, ClusterConfiguration config) {
        try {
            db.put(SnapshotMetadata.SNAPSHOT_LAST_INDEX_KEY.getBytes(), String.valueOf(lastIndex).getBytes());
            db.put(SnapshotMetadata.SNAPSHOT_LAST_TERM_KEY.getBytes(), String.valueOf(lastTerm).getBytes());
            db.put(SnapshotMetadata.SNAPSHOT_CONFIG_KEY.getBytes(), config.serialize().getBytes());
        } catch (RocksDBException e) {
            System.err.println("Failed to persist snapshot metadata: " + e.getMessage());
        }
    }

    private void restoreSnapshotMetadata() {
        try {
            byte[] indexBytes = db.get(SnapshotMetadata.SNAPSHOT_LAST_INDEX_KEY.getBytes());
            byte[] termBytes = db.get(SnapshotMetadata.SNAPSHOT_LAST_TERM_KEY.getBytes());
            byte[] configBytes = db.get(SnapshotMetadata.SNAPSHOT_CONFIG_KEY.getBytes());

            if (indexBytes != null && termBytes != null) {
                long snapIndex = Long.parseLong(new String(indexBytes));
                long snapTerm = Long.parseLong(new String(termBytes));

                log.discardUpTo(snapIndex, snapTerm);
                commitIndex = Math.max(commitIndex, snapIndex);
                lastApplied = Math.max(lastApplied, snapIndex);
                lastSnapshotIndex = snapIndex;

                if (configBytes != null) {
                    clusterConfig = ClusterConfiguration.deserialize(new String(configBytes));
                }

                System.out.println("🔄 [" + nodeId + "] Restored snapshot metadata: index=" + snapIndex + " term=" + snapTerm);
            }
        } catch (RocksDBException e) {
            System.err.println("⚠️ [" + nodeId + "] Failed to restore snapshot metadata: " + e.getMessage());
        }
    }

    // --- Restore Persisted State (Task 10.1: updated) ---

    private void restorePersistedState() {
        try {
            // Restore currentTerm
            byte[] termBytes = db.get(RAFT_TERM_KEY.getBytes());
            if (termBytes != null) {
                this.currentTerm = Long.parseLong(new String(termBytes));
                System.out.println("🔄 [" + nodeId + "] Restored term: " + currentTerm);
            }

            // Restore votedFor
            byte[] votedForBytes = db.get(RAFT_VOTED_FOR_KEY.getBytes());
            if (votedForBytes != null) {
                String val = new String(votedForBytes);
                this.votedFor = val.isEmpty() ? null : val;
                System.out.println("🔄 [" + nodeId + "] Restored votedFor: " + votedFor);
            }

            // Restore snapshot metadata BEFORE restoring log entries
            restoreSnapshotMetadata();

            // Restore log entries by scanning keys with the log prefix
            try (org.rocksdb.RocksIterator iterator = db.newIterator()) {
                iterator.seek(RAFT_LOG_PREFIX.getBytes());
                while (iterator.isValid()) {
                    String key = new String(iterator.key());
                    if (!key.startsWith(RAFT_LOG_PREFIX)) break;

                    String val = new String(iterator.value());
                    // Parse "term:key:value" — use indexOf to handle values containing ':'
                    int firstColon = val.indexOf(':');
                    int secondColon = val.indexOf(':', firstColon + 1);
                    if (firstColon > 0 && secondColon > firstColon) {
                        long term = Long.parseLong(val.substring(0, firstColon));
                        String entryKey = val.substring(firstColon + 1, secondColon);
                        String entryValue = val.substring(secondColon + 1);
                        log.append(new LogEntryData(term, entryKey, entryValue));
                    }
                    iterator.next();
                }
            }

            if (log.size() > 0) {
                System.out.println("🔄 [" + nodeId + "] Restored " + log.size() + " log entries (lastIndex=" + log.getLastIndex() + ")");
            }

            // Replay committed CONFIGURATION entries to restore clusterConfig (Task 10.1)
            for (long i = log.getLogOffset() + 1; i <= commitIndex; i++) {
                LogEntryData entry = log.getEntry(i);
                if (entry != null && entry.getType() == LogEntryData.EntryType.CONFIGURATION) {
                    clusterConfig = ClusterConfiguration.deserialize(entry.getValue());
                    System.out.println("🔄 [" + nodeId + "] Replayed config entry at index " + i);
                }
            }
        } catch (RocksDBException e) {
            System.err.println("⚠️ [" + nodeId + "] Failed to restore persisted state: " + e.getMessage());
        }
    }

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

        restorePersistedState();
        resetTimer();
        // NOTE: Election timer is NOT started here. Call start() after peers are registered.
    }

    /**
     * Start the election timer. Call this AFTER all peers have been registered
     * to prevent premature elections with zero peers.
     * Task 6.1: Initialize clusterConfig from peers list + self.
     */
    public void start() {
        if (clusterConfig == null) {
            Map<String, String> initialMembers = new HashMap<>();
            initialMembers.put(nodeId, ""); // self, address not needed for self
            for (RaftPeerClient peer : peers) {
                initialMembers.put(peer.getPeerId(), ""); // addresses filled by bootstrap
            }
            clusterConfig = new ClusterConfiguration(initialMembers);
        }
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

    public ReplicatedLog getLog() {
        return log;
    }

    public long getCommitIndex() {
        lock.readLock().lock();
        try { return commitIndex; } finally { lock.readLock().unlock(); }
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

    // --- Leader Entry Append ---

    /**
     * Leader-only: append a new log entry for a client write.
     * Stores the observer in pendingRequests and triggers replication.
     */
    public void appendEntry(String key, String value, StreamObserver<PutResponse> observer) {
        lock.writeLock().lock();
        try {
            if (this.state != State.LEADER) {
                // Reject if not leader
                PutResponse response = PutResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(knownLeader != null ? knownLeader : "UNKNOWN")
                        .build();
                observer.onNext(response);
                observer.onCompleted();
                return;
            }

            LogEntryData entry = new LogEntryData(currentTerm, key, value);
            long index = log.append(entry);
            persistLogEntry(index, entry);
            pendingRequests.put(index, observer);
            System.out.println("📝 [" + nodeId + "] Appended to log at index " + index + " (term " + currentTerm + ")");
        } finally {
            lock.writeLock().unlock();
        }

        // Trigger immediate replication outside the lock
        broadcastHeartbeats();
    }

    // --- State Transitions ---

    public void stepDown(long newTerm) {
        lock.writeLock().lock();
        try {
            this.currentTerm = newTerm;
            this.state = State.FOLLOWER;
            this.votedFor = null;
            persistTerm();
            persistVotedFor();
            // Cancel heartbeat task if we were leader
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
                heartbeatTask = null;
            }
            resetTimer();
            failPendingRequests();
        } finally { lock.writeLock().unlock(); }
    }

    public void grantVote(String candidateId) {
        lock.writeLock().lock();
        try {
            this.votedFor = candidateId;
            persistVotedFor();
            resetTimer();
        } finally { lock.writeLock().unlock(); }
    }

    /**
     * Public resetHeartbeat — acquires the write lock, then delegates.
     */
    public void resetHeartbeat(long leaderTerm, String leaderId) {
        lock.writeLock().lock();
        try {
            resetHeartbeatInternal(leaderTerm, leaderId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Internal resetHeartbeat — must be called while already holding the write lock.
     */
    private void resetHeartbeatInternal(long leaderTerm, String leaderId) {
        if (leaderTerm >= this.currentTerm) {
            this.knownLeader = leaderId;
            this.currentTerm = leaderTerm;
            this.state = State.FOLLOWER;
            persistTerm();
            persistVotedFor();
            // Cancel heartbeat task if we were leader
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
                heartbeatTask = null;
            }
            resetTimer();
        }
    }

    // --- Follower AppendEntries Handling ---

    /**
     * Full Raft AppendEntries receiver (§5.3).
     * Called by RaftInternalServiceImpl. Acquires write lock internally.
     */
    public AppendResponse handleAppendEntries(AppendRequest request) {
        lock.writeLock().lock();
        try {
            long requestTerm = request.getTerm();

            // 1. Reply false if term < currentTerm
            if (requestTerm < this.currentTerm) {
                return AppendResponse.newBuilder()
                        .setTerm(this.currentTerm)
                        .setSuccess(false)
                        .build();
            }

            // Valid leader — ALWAYS reset heartbeat timer to prevent unnecessary elections.
            // This must happen before any consistency checks so that even rejected
            // AppendEntries from a valid leader prevent election timeouts.
            resetHeartbeatInternal(requestTerm, request.getLeaderId());

            long prevLogIndex = request.getPrevLogIndex();
            long prevLogTerm = request.getPrevLogTerm();

            // 2. Reply false if log doesn't contain an entry at prevLogIndex with matching term
            if (prevLogIndex > 0) {
                if (prevLogIndex > log.getLastIndex()) {
                    // We don't have an entry at prevLogIndex at all
                    System.out.println("⚠️ [" + nodeId + "] Log too short: prevLogIndex=" + prevLogIndex + " but lastIndex=" + log.getLastIndex());
                    return AppendResponse.newBuilder()
                            .setTerm(this.currentTerm)
                            .setSuccess(false)
                            .build();
                }
                long localTerm = log.getTermAt(prevLogIndex);
                if (localTerm != prevLogTerm) {
                    // Term mismatch at prevLogIndex
                    System.out.println("⚠️ [" + nodeId + "] Term mismatch at index " + prevLogIndex + ": local=" + localTerm + " leader=" + prevLogTerm);
                    return AppendResponse.newBuilder()
                            .setTerm(this.currentTerm)
                            .setSuccess(false)
                            .build();
                }
            }

            // 3 & 4. Process entries: conflict resolution and append
            List<com.auditraft.grpc.LogEntry> entries = request.getEntriesList();
            long lastNewEntryIndex = prevLogIndex;

            for (int i = 0; i < entries.size(); i++) {
                com.auditraft.grpc.LogEntry entryProto = entries.get(i);
                long entryIndex = prevLogIndex + 1 + i;
                long existingTerm = log.getTermAt(entryIndex);

                if (existingTerm != 0 && entryIndex <= log.getLastIndex()) {
                    // Entry exists at this index
                    if (existingTerm != entryProto.getTerm()) {
                        // Conflict: delete this entry and all that follow
                        long oldLastIndex = log.getLastIndex();
                        log.truncateFrom(entryIndex);
                        // Delete persisted entries from entryIndex to oldLastIndex
                        for (long idx = entryIndex; idx <= oldLastIndex; idx++) {
                            deleteLogEntry(idx);
                        }
                        // Append the new entry
                        LogEntryData newEntry = new LogEntryData(entryProto.getTerm(), entryProto.getKey(), entryProto.getValue());
                        log.append(newEntry);
                        persistLogEntry(entryIndex, newEntry);
                    }
                    // else: same term, entry already present — skip
                } else {
                    // No entry at this index — append
                    LogEntryData newEntry = new LogEntryData(entryProto.getTerm(), entryProto.getKey(), entryProto.getValue());
                    log.append(newEntry);
                    persistLogEntry(entryIndex, newEntry);
                }
                lastNewEntryIndex = entryIndex;
            }

            // 5. Update commitIndex
            if (request.getLeaderCommit() > this.commitIndex) {
                if (entries.isEmpty()) {
                    // Pure heartbeat — advance commitIndex to leaderCommit but not beyond our log
                    this.commitIndex = Math.min(request.getLeaderCommit(), log.getLastIndex());
                } else {
                    this.commitIndex = Math.min(request.getLeaderCommit(), lastNewEntryIndex);
                }
                applyCommittedEntries();
            }

            return AppendResponse.newBuilder()
                    .setTerm(this.currentTerm)
                    .setSuccess(true)
                    .build();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // --- Leader AppendResponse Handling (Task 12.2: updated with catch-up progress) ---

    /**
     * Handle a response from a follower to an AppendEntries RPC.
     * Called from the async gRPC callback. Acquires write lock internally.
     */
    public void handleAppendResponse(String peerId, boolean success, long responseTerm, long lastEntryIndex) {
        lock.writeLock().lock();
        try {
            if (this.state != State.LEADER) return;

            // If response term is higher, step down
            if (responseTerm > this.currentTerm) {
                this.currentTerm = responseTerm;
                this.state = State.FOLLOWER;
                this.votedFor = null;
                persistTerm();
                persistVotedFor();
                resetTimer();
                failPendingRequests();
                return;
            }

            if (success) {
                // Update matchIndex and nextIndex for this peer
                matchIndex.put(peerId, lastEntryIndex);
                nextIndex.put(peerId, lastEntryIndex + 1);
                tryAdvanceCommitIndex();

                // Task 12.2: Notify catch-up progress if this is the catch-up peer
                onCatchUpProgress(peerId, lastEntryIndex);
            } else {
                // Decrement nextIndex for this peer (minimum 1)
                long currentNext = nextIndex.getOrDefault(peerId, 1L);
                nextIndex.put(peerId, Math.max(currentNext - 1, 1));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // --- Commit Index Advancement ---

    /**
     * Scan for the highest N > commitIndex where a majority of nodes
     * (including the leader) have matchIndex >= N and log[N].term == currentTerm.
     * Must be called under write lock.
     */
    private void tryAdvanceCommitIndex() {
        long lastIndex = log.getLastIndex();
        for (long n = lastIndex; n > commitIndex; n--) {
            if (log.getTermAt(n) != this.currentTerm) {
                continue;
            }
            // Count replicas: leader always has the entry
            int replicaCount = 1;
            for (Long mi : matchIndex.values()) {
                if (mi >= n) {
                    replicaCount++;
                }
            }
            int majority = (peers.size() + 1) / 2 + 1;
            if (replicaCount >= majority) {
                commitIndex = n;
                applyCommittedEntries();
                break;
            }
        }
    }

    // --- State Machine Application (Task 6.5: updated for config entries, Task 5.2: snapshot) ---

    /**
     * Apply committed entries from lastApplied+1 through commitIndex to RocksDB.
     * Complete pending client observers on success.
     * Handles CONFIGURATION entries by updating clusterConfig instead of writing to state machine.
     * Must be called under write lock.
     */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntryData entry = log.getEntry(lastApplied);
            if (entry == null) break;

            // Task 6.5: Handle CONFIGURATION entries
            if (entry.getType() == LogEntryData.EntryType.CONFIGURATION) {
                // Apply configuration change
                ClusterConfiguration newConfig = ClusterConfiguration.deserialize(entry.getValue());
                this.clusterConfig = newConfig;
                System.out.println("🔧 [" + nodeId + "] Applied config change at index " + lastApplied);

                // Complete pending config observer if this was our pending change
                if (pendingConfigChange && pendingConfigObserver != null) {
                    try {
                        pendingConfigObserver = null;
                        pendingConfigChange = false;
                    } catch (Exception e) { /* ignore */ }
                    pendingConfigChange = false;
                    pendingConfigObserver = null;
                }

                // Check if we (the leader) were removed
                if (state == State.LEADER && !newConfig.containsMember(nodeId)) {
                    System.out.println("👋 [" + nodeId + "] I was removed from the cluster, stepping down");
                    this.state = State.FOLLOWER;
                    if (heartbeatTask != null) { heartbeatTask.cancel(false); heartbeatTask = null; }
                    failPendingRequests();
                }

                // Don't write config entries to the application state machine
                continue; // skip the db.put below
            }

            // DATA entries: apply to RocksDB state machine
            try {
                db.put(entry.getKey().getBytes(), entry.getValue().getBytes());
                System.out.println("✅ [" + nodeId + "] Applied entry at index " + lastApplied
                        + ": " + entry.getKey() + "=" + entry.getValue());
            } catch (RocksDBException e) {
                System.err.println("❌ [" + nodeId + "] Failed to apply entry at index " + lastApplied);
                lastApplied--;
                break;
            }

            // Complete pending client observer if present
            StreamObserver<PutResponse> observer = pendingRequests.remove(lastApplied);
            if (observer != null) {
                try {
                    PutResponse response = PutResponse.newBuilder()
                            .setSuccess(true)
                            .setLeaderId(this.nodeId)
                            .build();
                    observer.onNext(response);
                    observer.onCompleted();
                } catch (Exception e) {
                    // Observer may have been cancelled
                    System.err.println("⚠️ [" + nodeId + "] Failed to complete observer at index " + lastApplied);
                }
            }
        }

        // Task 5.2: Check if we should create a snapshot after applying entries
        maybeSnapshot();
    }

    // --- Snapshot Creation (Task 5.2) ---

    private void maybeSnapshot() {
        if ((lastApplied - lastSnapshotIndex) >= snapshotThreshold) {
            createSnapshot();
        }
    }

    private void createSnapshot() {
        long snapIndex = lastApplied;
        long snapTerm = log.getTermAt(snapIndex);
        if (snapTerm == 0 && snapIndex > 0) return; // safety check

        persistSnapshotMetadata(snapIndex, snapTerm, clusterConfig != null ? clusterConfig : new ClusterConfiguration(new HashMap<>()));

        // Delete persisted log entries up to snapIndex
        for (long i = log.getLogOffset() + 1; i <= snapIndex; i++) {
            deleteLogEntry(i);
        }

        log.discardUpTo(snapIndex, snapTerm);
        lastSnapshotIndex = snapIndex;
        System.out.println("📸 [" + nodeId + "] Created snapshot at index " + snapIndex + " (term " + snapTerm + ")");
    }

    // --- Pending Request Lifecycle (Task 12.1: updated for config cleanup) ---

    /**
     * Fail all pending client requests (e.g., on step-down).
     * Also cleans up pending config changes and catch-up state.
     * Must be called under write lock.
     */
    private void failPendingRequests() {
        String leader = knownLeader != null ? knownLeader : "UNKNOWN";
        for (Map.Entry<Long, StreamObserver<PutResponse>> entry : pendingRequests.entrySet()) {
            try {
                PutResponse response = PutResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(leader)
                        .build();
                entry.getValue().onNext(response);
                entry.getValue().onCompleted();
            } catch (Exception e) {
                // Observer may have been cancelled
            }
        }
        pendingRequests.clear();

        // Task 12.1: Fail pending config on step-down
        if (pendingConfigChange && pendingConfigObserver != null) {
            try { pendingConfigObserver.onCompleted(); } catch (Exception e) {}
            pendingConfigObserver = null;
        }
        pendingConfigChange = false;
        if (activeCatchUp != null) {
            peers.remove(activeCatchUp.getPeerClient());
            activeCatchUp.getPeerClient().shutdown();
            activeCatchUp = null;
        }
    }

    // --- Election Safety: Log Up-to-Date Check ---

    /**
     * Raft §5.4.1: Returns true if the candidate's log is at least as up-to-date as this node's log.
     */
    public boolean isLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        lock.readLock().lock();
        try {
            long myLastTerm = log.getLastTerm();
            long myLastIndex = log.getLastIndex();

            if (candidateLastTerm != myLastTerm) {
                return candidateLastTerm > myLastTerm;
            }
            return candidateLastIndex >= myLastIndex;
        } finally {
            lock.readLock().unlock();
        }
    }

    // --- Membership Change: AddServer (Task 6.2) ---

    public void handleAddServer(com.auditraft.grpc.AddServerRequest request, StreamObserver<com.auditraft.grpc.AddServerResponse> observer) {
        lock.writeLock().lock();
        try {
            if (state != State.LEADER) {
                observer.onNext(com.auditraft.grpc.AddServerResponse.newBuilder()
                    .setSuccess(false).setStatus("not leader")
                    .setLeaderId(knownLeader != null ? knownLeader : "UNKNOWN").build());
                observer.onCompleted();
                return;
            }
            if (pendingConfigChange) {
                observer.onNext(com.auditraft.grpc.AddServerResponse.newBuilder()
                    .setSuccess(false).setStatus("membership change in progress")
                    .setLeaderId(nodeId).build());
                observer.onCompleted();
                return;
            }
            if (clusterConfig != null && clusterConfig.containsMember(request.getServerId())) {
                observer.onNext(com.auditraft.grpc.AddServerResponse.newBuilder()
                    .setSuccess(false).setStatus("server already a member")
                    .setLeaderId(nodeId).build());
                observer.onCompleted();
                return;
            }
            pendingConfigChange = true;
            pendingConfigObserver = observer;
        } finally { lock.writeLock().unlock(); }

        startCatchUp(request.getServerId(), request.getAddress());
    }

    // --- Membership Change: Catch-Up Phase (Task 6.3) ---

    private void startCatchUp(String serverId, String address) {
        String[] parts = address.split(":");
        RaftPeerClient catchUpClient = new RaftPeerClient(serverId, parts[0], Integer.parseInt(parts[1]));

        lock.writeLock().lock();
        try {
            activeCatchUp = new CatchUpState(serverId, address, catchUpClient, log.getLastIndex() + 1);
        } finally { lock.writeLock().unlock(); }

        // Replication to the catch-up node happens via the heartbeat loop
        // We add it temporarily to the peers list for replication
        peers.add(catchUpClient);
        System.out.println("🔄 [" + nodeId + "] Started catch-up for " + serverId + " at " + address);
    }

    // Called from handleAppendResponse when the catch-up peer responds
    private void onCatchUpProgress(String peerId, long peerMatchIndex) {
        if (activeCatchUp == null || !activeCatchUp.getServerId().equals(peerId)) return;

        long previousMatch = activeCatchUp.getMatchIndex();
        activeCatchUp.setMatchIndex(peerMatchIndex);
        activeCatchUp.setNextIndex(peerMatchIndex + 1);
        activeCatchUp.setRoundsCompleted(activeCatchUp.getRoundsCompleted() + 1);

        if (peerMatchIndex > previousMatch) {
            activeCatchUp.setRoundsWithoutProgress(0);
        } else {
            activeCatchUp.setRoundsWithoutProgress(activeCatchUp.getRoundsWithoutProgress() + 1);
        }

        // Check if caught up
        if (log.getLastIndex() - peerMatchIndex <= activeCatchUp.getProgressThreshold()) {
            // Caught up! Append configuration entry
            ClusterConfiguration newConfig = new ClusterConfiguration(clusterConfig);
            newConfig.addMember(activeCatchUp.getServerId(), activeCatchUp.getAddress());

            LogEntryData configEntry = new LogEntryData(currentTerm, "__config__", newConfig.serialize(), LogEntryData.EntryType.CONFIGURATION);
            long index = log.append(configEntry);
            persistLogEntry(index, configEntry);

            activeCatchUp = null;
            System.out.println("✅ [" + nodeId + "] Catch-up complete for " + peerId + ", config entry appended at index " + index);
            return;
        }

        // Check if too many rounds without progress
        if (activeCatchUp.getRoundsWithoutProgress() >= activeCatchUp.getMaxRounds()) {
            abortCatchUp("catch-up timeout: no progress after " + activeCatchUp.getMaxRounds() + " rounds");
        }
    }

    private void abortCatchUp(String reason) {
        if (activeCatchUp != null) {
            peers.remove(activeCatchUp.getPeerClient());
            activeCatchUp.getPeerClient().shutdown();
            activeCatchUp = null;
        }
        pendingConfigChange = false;
        if (pendingConfigObserver != null) {
            try {
                // Cast to the correct type
                @SuppressWarnings("unchecked")
                StreamObserver<com.auditraft.grpc.AddServerResponse> obs =
                    (StreamObserver<com.auditraft.grpc.AddServerResponse>) pendingConfigObserver;
                obs.onNext(com.auditraft.grpc.AddServerResponse.newBuilder()
                    .setSuccess(false).setStatus(reason).setLeaderId(nodeId).build());
                obs.onCompleted();
            } catch (Exception e) { /* observer may be cancelled */ }
            pendingConfigObserver = null;
        }
        System.out.println("❌ [" + nodeId + "] Catch-up aborted: " + reason);
    }

    // --- Membership Change: RemoveServer (Task 6.4) ---

    public void handleRemoveServer(com.auditraft.grpc.RemoveServerRequest request, StreamObserver<com.auditraft.grpc.RemoveServerResponse> observer) {
        lock.writeLock().lock();
        try {
            if (state != State.LEADER) {
                observer.onNext(com.auditraft.grpc.RemoveServerResponse.newBuilder()
                    .setSuccess(false).setStatus("not leader")
                    .setLeaderId(knownLeader != null ? knownLeader : "UNKNOWN").build());
                observer.onCompleted();
                return;
            }
            if (pendingConfigChange) {
                observer.onNext(com.auditraft.grpc.RemoveServerResponse.newBuilder()
                    .setSuccess(false).setStatus("membership change in progress")
                    .setLeaderId(nodeId).build());
                observer.onCompleted();
                return;
            }
            if (clusterConfig == null || !clusterConfig.containsMember(request.getServerId())) {
                observer.onNext(com.auditraft.grpc.RemoveServerResponse.newBuilder()
                    .setSuccess(false).setStatus("server not a member")
                    .setLeaderId(nodeId).build());
                observer.onCompleted();
                return;
            }

            ClusterConfiguration newConfig = new ClusterConfiguration(clusterConfig);
            newConfig.removeMember(request.getServerId());

            LogEntryData configEntry = new LogEntryData(currentTerm, "__config__", newConfig.serialize(), LogEntryData.EntryType.CONFIGURATION);
            long index = log.append(configEntry);
            persistLogEntry(index, configEntry);

            pendingConfigChange = true;
            pendingConfigObserver = observer;
            System.out.println("📝 [" + nodeId + "] RemoveServer config entry appended at index " + index);
        } finally { lock.writeLock().unlock(); }

        broadcastHeartbeats();
    }

    // --- InstallSnapshot Handler (Task 8.2) ---

    public com.auditraft.grpc.InstallSnapshotResponse handleInstallSnapshot(com.auditraft.grpc.InstallSnapshotRequest request) {
        lock.writeLock().lock();
        try {
            if (request.getTerm() < currentTerm) {
                return com.auditraft.grpc.InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
            }

            if (request.getTerm() > currentTerm) {
                currentTerm = request.getTerm();
                state = State.FOLLOWER;
                votedFor = null;
                persistTerm();
                persistVotedFor();
            }

            resetHeartbeatInternal(request.getTerm(), request.getLeaderId());

            // If we're already ahead, discard
            if (request.getLastIncludedIndex() <= commitIndex) {
                return com.auditraft.grpc.InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
            }

            // Clear application data from RocksDB and write snapshot data
            byte[] data = request.getData().toByteArray();
            clearApplicationData();
            applySnapshotData(data);

            // Update log
            log.discardUpTo(request.getLastIncludedIndex(), request.getLastIncludedTerm());

            // Update state
            commitIndex = request.getLastIncludedIndex();
            lastApplied = request.getLastIncludedIndex();
            lastSnapshotIndex = request.getLastIncludedIndex();

            // Restore cluster config from snapshot
            if (!request.getClusterConfiguration().isEmpty()) {
                clusterConfig = ClusterConfiguration.deserialize(request.getClusterConfiguration());
            }

            // Persist snapshot metadata
            persistSnapshotMetadata(request.getLastIncludedIndex(), request.getLastIncludedTerm(),
                clusterConfig != null ? clusterConfig : new ClusterConfiguration(new HashMap<>()));

            System.out.println("📥 [" + nodeId + "] Installed snapshot at index " + request.getLastIncludedIndex());
            return com.auditraft.grpc.InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
        } finally { lock.writeLock().unlock(); }
    }

    private void clearApplicationData() {
        try (org.rocksdb.RocksIterator it = db.newIterator()) {
            it.seekToFirst();
            while (it.isValid()) {
                String key = new String(it.key());
                if (!key.startsWith("__raft_") && !key.startsWith("__snapshot_")) {
                    db.delete(it.key());
                }
                it.next();
            }
        } catch (RocksDBException e) {
            System.err.println("Failed to clear application data: " + e.getMessage());
        }
    }

    private void applySnapshotData(byte[] data) {
        // Parse length-prefixed key-value pairs
        int offset = 0;
        while (offset + 4 <= data.length) {
            int keyLen = ((data[offset] & 0xFF) << 24) | ((data[offset+1] & 0xFF) << 16) |
                         ((data[offset+2] & 0xFF) << 8) | (data[offset+3] & 0xFF);
            offset += 4;
            if (offset + keyLen > data.length) break;
            byte[] key = new byte[keyLen];
            System.arraycopy(data, offset, key, 0, keyLen);
            offset += keyLen;

            if (offset + 4 > data.length) break;
            int valLen = ((data[offset] & 0xFF) << 24) | ((data[offset+1] & 0xFF) << 16) |
                         ((data[offset+2] & 0xFF) << 8) | (data[offset+3] & 0xFF);
            offset += 4;
            if (offset + valLen > data.length) break;
            byte[] value = new byte[valLen];
            System.arraycopy(data, offset, value, 0, valLen);
            offset += valLen;

            try { db.put(key, value); } catch (RocksDBException e) { /* log error */ }
        }
    }

    private byte[] buildSnapshotData() {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (org.rocksdb.RocksIterator it = db.newIterator()) {
            it.seekToFirst();
            while (it.isValid()) {
                String key = new String(it.key());
                if (!key.startsWith("__raft_") && !key.startsWith("__snapshot_")) {
                    byte[] k = it.key();
                    byte[] v = it.value();
                    // Write length-prefixed key
                    baos.write((k.length >> 24) & 0xFF);
                    baos.write((k.length >> 16) & 0xFF);
                    baos.write((k.length >> 8) & 0xFF);
                    baos.write(k.length & 0xFF);
                    baos.write(k);
                    // Write length-prefixed value
                    baos.write((v.length >> 24) & 0xFF);
                    baos.write((v.length >> 16) & 0xFF);
                    baos.write((v.length >> 8) & 0xFF);
                    baos.write(v.length & 0xFF);
                    baos.write(v);
                }
                it.next();
            }
        } catch (Exception e) {
            System.err.println("Failed to build snapshot data: " + e.getMessage());
        }
        return baos.toByteArray();
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

        long lastLogIndex;
        long lastLogTerm;

        lock.writeLock().lock();
        try {
            this.state = State.CANDIDATE;
            this.currentTerm++;
            this.votedFor = this.nodeId;
            persistTerm();
            persistVotedFor();
            resetTimer();
            electionTerm = this.currentTerm;
            lastLogIndex = log.getLastIndex();
            lastLogTerm = log.getLastTerm();
            System.out.println("\n🗳️ [" + nodeId + "] Election timeout! Started election for term " + currentTerm);
        } finally { lock.writeLock().unlock(); }

        VoteRequest request = VoteRequest.newBuilder()
                .setTerm(electionTerm).setCandidateId(this.nodeId)
                .setLastLogIndex(lastLogIndex).setLastLogTerm(lastLogTerm).build();

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
                @Override public void onError(Throwable t) {}
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

            // Initialize nextIndex and matchIndex for each peer
            long lastIndex = log.getLastIndex();
            for (RaftPeerClient peer : peers) {
                nextIndex.put(peer.getPeerId(), lastIndex + 1);
                matchIndex.put(peer.getPeerId(), 0L);
            }

            System.out.println("👑 [" + nodeId + "] Won election! Now LEADER for term " + currentTerm);
            // Cancel any previous heartbeat task to prevent accumulation
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
            }
            heartbeatTask = timerExecutor.scheduleAtFixedRate(this::broadcastHeartbeats, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
        } finally { lock.writeLock().unlock(); }
    }

    /**
     * Build a per-follower AppendRequest with correct prevLogIndex/prevLogTerm/entries/leaderCommit.
     * Returns null if the peer needs an InstallSnapshot instead (peerNextIndex <= logOffset).
     * Must be called under at least the read lock.
     */
    private AppendRequest buildAppendRequestForPeer(String peerId, long currentTermSnapshot) {
        long peerNextIndex = nextIndex.getOrDefault(peerId, 1L);

        // Task 8.3: If peer needs entries we've already compacted, signal InstallSnapshot
        if (peerNextIndex <= log.getLogOffset()) {
            return null; // signal to send InstallSnapshot instead
        }

        long prevLogIdx = peerNextIndex - 1;
        long prevLogTrm = log.getTermAt(prevLogIdx);
        long lastIndex = log.getLastIndex();

        AppendRequest.Builder builder = AppendRequest.newBuilder()
                .setTerm(currentTermSnapshot)
                .setLeaderId(this.nodeId)
                .setPrevLogIndex(prevLogIdx)
                .setPrevLogTerm(prevLogTrm)
                .setLeaderCommit(this.commitIndex);

        // Add entries from nextIndex through end of log
        if (peerNextIndex <= lastIndex) {
            List<LogEntryData> entriesToSend = log.getEntriesFrom(peerNextIndex);
            for (int i = 0; i < entriesToSend.size(); i++) {
                LogEntryData e = entriesToSend.get(i);
                long entryIndex = peerNextIndex + i;
                com.auditraft.grpc.LogEntry protoEntry = com.auditraft.grpc.LogEntry.newBuilder()
                        .setTerm(e.getTerm())
                        .setIndex(entryIndex)
                        .setKey(e.getKey())
                        .setValue(e.getValue())
                        .build();
                builder.addEntries(protoEntry);
            }
        }

        return builder.build();
    }

    /**
     * Broadcast heartbeats (AppendEntries) to all peers.
     * Task 8.3: When a peer needs entries that have been compacted away,
     * send an InstallSnapshot RPC instead.
     */
    private void broadcastHeartbeats() {
        long currentTermSnapshot;
        List<RaftPeerClient> peerSnapshot;

        // We need to build requests under the read lock, but some peers may need
        // InstallSnapshot which requires building snapshot data under the lock too.
        // Collect what we need, then send outside the lock.
        List<Object> requestObjects = new ArrayList<>(); // AppendRequest or InstallSnapshotRequest
        List<String> peerIds = new ArrayList<>();

        lock.readLock().lock();
        try {
            if (this.state != State.LEADER) return;
            currentTermSnapshot = this.currentTerm;
            peerSnapshot = new ArrayList<>(peers);

            for (RaftPeerClient peer : peerSnapshot) {
                String peerId = peer.getPeerId();
                peerIds.add(peerId);
                AppendRequest appendReq = buildAppendRequestForPeer(peerId, currentTermSnapshot);
                if (appendReq != null) {
                    requestObjects.add(appendReq);
                } else {
                    // Peer needs InstallSnapshot
                    byte[] snapshotData = buildSnapshotData();
                    com.auditraft.grpc.InstallSnapshotRequest snapReq = com.auditraft.grpc.InstallSnapshotRequest.newBuilder()
                            .setTerm(currentTermSnapshot)
                            .setLeaderId(this.nodeId)
                            .setLastIncludedIndex(log.getLogOffset())
                            .setLastIncludedTerm(log.getLastIncludedTerm())
                            .setData(ByteString.copyFrom(snapshotData))
                            .setClusterConfiguration(clusterConfig != null ? clusterConfig.serialize() : "")
                            .build();
                    requestObjects.add(snapReq);
                }
            }
        } finally { lock.readLock().unlock(); }

        // Send RPCs outside the lock
        for (int i = 0; i < peerSnapshot.size(); i++) {
            RaftPeerClient peer = peerSnapshot.get(i);
            String peerId = peerIds.get(i);
            Object reqObj = requestObjects.get(i);

            if (reqObj instanceof AppendRequest) {
                AppendRequest request = (AppendRequest) reqObj;
                // Calculate the last entry index we're sending to this peer
                long lastEntryIndex;
                if (request.getEntriesCount() > 0) {
                    lastEntryIndex = request.getPrevLogIndex() + request.getEntriesCount();
                } else {
                    lastEntryIndex = request.getPrevLogIndex();
                }
                final long capturedLastEntryIndex = lastEntryIndex;

                peer.sendAppendEntries(request, new StreamObserver<AppendResponse>() {
                    @Override public void onNext(AppendResponse response) {
                        handleAppendResponse(peerId, response.getSuccess(), response.getTerm(), capturedLastEntryIndex);
                    }
                    @Override public void onError(Throwable t) {}
                    @Override public void onCompleted() {}
                });
            } else if (reqObj instanceof com.auditraft.grpc.InstallSnapshotRequest) {
                com.auditraft.grpc.InstallSnapshotRequest snapReq = (com.auditraft.grpc.InstallSnapshotRequest) reqObj;
                final long snapLastIndex = snapReq.getLastIncludedIndex();

                peer.sendInstallSnapshot(snapReq, new StreamObserver<com.auditraft.grpc.InstallSnapshotResponse>() {
                    @Override public void onNext(com.auditraft.grpc.InstallSnapshotResponse response) {
                        lock.writeLock().lock();
                        try {
                            if (response.getTerm() > currentTerm) {
                                currentTerm = response.getTerm();
                                state = State.FOLLOWER;
                                votedFor = null;
                                persistTerm();
                                persistVotedFor();
                                resetTimer();
                                failPendingRequests();
                                return;
                            }
                            // Snapshot accepted — update nextIndex/matchIndex
                            nextIndex.put(peerId, snapLastIndex + 1);
                            matchIndex.put(peerId, snapLastIndex);
                        } finally { lock.writeLock().unlock(); }
                    }
                    @Override public void onError(Throwable t) {}
                    @Override public void onCompleted() {}
                });
            }
        }
    }
}
