package com.auditraft.core;

/**
 * Immutable value object representing a single entry in the replicated log.
 * Named LogEntryData to avoid collision with the proto-generated
 * {@link com.auditraft.grpc.LogEntry}.
 */
public class LogEntryData {

    public enum EntryType { DATA, CONFIGURATION }

    private final long term;
    private final String key;
    private final String value;
    private final EntryType type;

    /** Backward-compatible constructor; defaults to DATA entry type. */
    public LogEntryData(long term, String key, String value) {
        this(term, key, value, EntryType.DATA);
    }

    public LogEntryData(long term, String key, String value, EntryType type) {
        this.term = term;
        this.key = key;
        this.value = value;
        this.type = type;
    }

    public long getTerm() {
        return term;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public EntryType getType() {
        return type;
    }
}
