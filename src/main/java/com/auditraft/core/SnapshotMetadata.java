package com.auditraft.core;

/**
 * Holds metadata about the most recent snapshot: the last log index and term
 * included in the snapshot, plus the cluster configuration at that point.
 *
 * Snapshot metadata is persisted to RocksDB under well-known keys so that
 * it survives node restarts.
 */
public class SnapshotMetadata {

    /** RocksDB key for the last included log index. */
    public static final String SNAPSHOT_LAST_INDEX_KEY = "__snapshot_last_index__";

    /** RocksDB key for the last included log term. */
    public static final String SNAPSHOT_LAST_TERM_KEY = "__snapshot_last_term__";

    /** RocksDB key for the serialized cluster configuration. */
    public static final String SNAPSHOT_CONFIG_KEY = "__snapshot_config__";

    private final long lastIncludedIndex;
    private final long lastIncludedTerm;
    private final ClusterConfiguration clusterConfiguration;

    public SnapshotMetadata(long lastIncludedIndex, long lastIncludedTerm,
                            ClusterConfiguration clusterConfiguration) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.clusterConfiguration = clusterConfiguration;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public ClusterConfiguration getClusterConfiguration() {
        return clusterConfiguration;
    }
}
