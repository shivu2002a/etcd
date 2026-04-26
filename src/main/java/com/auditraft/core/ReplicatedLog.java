package com.auditraft.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An in-memory ordered log with 1-based indexing and log offset support for compaction.
 * <p>
 * After a snapshot, entries before the snapshot point are discarded. The {@code logOffset}
 * tracks the index of the last discarded entry (the last included index from the snapshot).
 * All index operations translate between logical (1-based Raft) indices and physical
 * (0-based ArrayList) indices using: {@code physicalIndex = logicalIndex - logOffset - 1}.
 * <p>
 * Not independently synchronized — always accessed under RaftNode's lock.
 */
public class ReplicatedLog {
    private final List<LogEntryData> entries = new ArrayList<>();
    private long logOffset = 0;
    private long lastIncludedTerm = 0;

    /**
     * Append an entry to the log.
     *
     * @return the assigned 1-based logical index
     */
    public long append(LogEntryData entry) {
        entries.add(entry);
        return logOffset + entries.size(); // logical 1-based index
    }

    /**
     * Get the entry at the given 1-based logical index.
     *
     * @return the entry, or null if the index is at or below the log offset or out of range
     */
    public LogEntryData getEntry(long index) {
        if (index <= logOffset) return null;
        int physical = (int)(index - logOffset - 1);
        if (physical < 0 || physical >= entries.size()) return null;
        return entries.get(physical);
    }

    /**
     * Get the term of the entry at the given 1-based logical index.
     *
     * @return the term, or 0 if index is 0 or out of range.
     *         Returns {@code lastIncludedTerm} if index equals the log offset.
     */
    public long getTermAt(long index) {
        if (index == 0) return 0;
        if (index == logOffset) return lastIncludedTerm;
        if (index < logOffset) return 0;
        LogEntryData entry = getEntry(index);
        return entry != null ? entry.getTerm() : 0;
    }

    /**
     * @return the 1-based logical index of the last entry, accounting for the log offset
     */
    public long getLastIndex() {
        return logOffset + entries.size();
    }

    /**
     * @return the term of the last entry, or {@code lastIncludedTerm} if the log is empty
     */
    public long getLastTerm() {
        if (entries.isEmpty()) return lastIncludedTerm;
        return entries.get(entries.size() - 1).getTerm();
    }

    /**
     * Get entries from startIndex (inclusive) through the end of the log.
     * Uses 1-based logical indexing. If startIndex is at or below the log offset,
     * it is adjusted to the first available entry.
     *
     * @return a new list of entries, or an empty list if no entries are available
     */
    public List<LogEntryData> getEntriesFrom(long startIndex) {
        if (startIndex <= logOffset) startIndex = logOffset + 1;
        int physical = (int)(startIndex - logOffset - 1);
        if (physical < 0 || physical >= entries.size()) return Collections.emptyList();
        return new ArrayList<>(entries.subList(physical, entries.size()));
    }

    /**
     * Delete all entries from the given 1-based logical index onward (inclusive).
     * Adjusted for the log offset.
     */
    public void truncateFrom(long index) {
        if (index <= logOffset) return;
        int physical = (int)(index - logOffset - 1);
        if (physical < 0 || physical >= entries.size()) return;
        entries.subList(physical, entries.size()).clear();
    }

    /**
     * Discard all log entries up to and including the given index, setting the
     * log offset and last included term for snapshot support.
     *
     * @param index the last included index from the snapshot
     * @param term  the term of the entry at the given index
     */
    public void discardUpTo(long index, long term) {
        if (index <= logOffset) return;
        int entriesToRemove = (int)(index - logOffset);
        if (entriesToRemove >= entries.size()) {
            entries.clear();
        } else {
            entries.subList(0, entriesToRemove).clear();
        }
        this.logOffset = index;
        this.lastIncludedTerm = term;
    }

    /**
     * @return the number of retained entries in the log (not counting discarded prefix)
     */
    public int size() {
        return entries.size();
    }

    /**
     * @return the log offset (last included index from the most recent snapshot)
     */
    public long getLogOffset() {
        return logOffset;
    }

    /**
     * @return the term of the entry at the log offset
     */
    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }
}
