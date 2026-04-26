package com.auditraft.core;

import com.auditraft.rpc.RaftPeerClient;

/**
 * Tracks the state of a catch-up replication session for a new server
 * being added to the cluster via AddServer.
 *
 * The leader replicates log entries to the new server in rounds. Once the
 * new server's matchIndex is within {@code progressThreshold} of the leader's
 * last log index, the catch-up is considered complete and a configuration
 * entry is appended. If too many rounds pass without progress, the catch-up
 * is aborted.
 */
public class CatchUpState {

    private final String serverId;
    private final String address;
    private final RaftPeerClient peerClient;
    private long nextIndex;
    private long matchIndex;
    private int roundsCompleted;
    private int roundsWithoutProgress;
    private final int maxRounds;
    private final long progressThreshold;

    public CatchUpState(String serverId, String address, RaftPeerClient peerClient,
                        long initialNextIndex) {
        this.serverId = serverId;
        this.address = address;
        this.peerClient = peerClient;
        this.nextIndex = initialNextIndex;
        this.matchIndex = 0;
        this.roundsCompleted = 0;
        this.roundsWithoutProgress = 0;
        this.maxRounds = 10;
        this.progressThreshold = 100;
    }

    public String getServerId() {
        return serverId;
    }

    public String getAddress() {
        return address;
    }

    public RaftPeerClient getPeerClient() {
        return peerClient;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public int getRoundsCompleted() {
        return roundsCompleted;
    }

    public void setRoundsCompleted(int roundsCompleted) {
        this.roundsCompleted = roundsCompleted;
    }

    public int getRoundsWithoutProgress() {
        return roundsWithoutProgress;
    }

    public void setRoundsWithoutProgress(int roundsWithoutProgress) {
        this.roundsWithoutProgress = roundsWithoutProgress;
    }

    public int getMaxRounds() {
        return maxRounds;
    }

    public long getProgressThreshold() {
        return progressThreshold;
    }
}
