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
public class CatchUpSta