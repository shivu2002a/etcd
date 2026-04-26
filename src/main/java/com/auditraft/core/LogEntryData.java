package com.auditraft.core;

/**
 * Immutable value object representing a single entry in the replicated log.
 * Named LogEntryData to avoid collision with the proto-generated
 * {@link com.auditraft.grpc.LogEntry}.
 */
public class LogEntryData {
    private final long term;
    private final String key;
    private final String value;

    public LogEntryData(long term, String key, String value) {
        this.term = term;
        this.key = key;
        this.value = value;
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
}
