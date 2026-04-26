package com.auditraft.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An in-memory ordered log with 1-based indexing.
 * List index 0 corresponds to log index 1.
 * <p>
 * Not independently synchronized — always accessed under RaftNode's lock.
 */
public class ReplicatedLog {
    private final List<LogEntryData> entries = new ArrayList<>();

    /**
     * Append an entry to the log.
     *
     * @return the assigned 1-based index
     */
    public long append(LogEntryData entry) {
        entries.add(entry);
        return entries.size(); // 1-based: size after add == new entry's index
    }

    /**
     * Get the entry at the given 1-based index.
     *
     * @return the entry, or null if the index is out of range
     */
    public LogEntryData getEntry(long index) {
        int i = (int) index - 1; // convert to 0-based
        if (i < 0 || i >= entries.size()) {
            return null;
        }
        return entries.get(i);
    }

    /**
     * Get the term of the entry at the given 1-based index.
     *
     * @return the term, or 0 if index is 0 or out of range
     */
    public long getTermAt(long index) {
        if (index == 0) {
            return 0;
        }
        LogEntryData entry = getEntry(index);
        return entry != null ? entry.getTerm() : 0;
    }

    /**
     * @return the 1-based index of the last entry, or 0 if the log is empty
     */
    public long getLastIndex() {
        return entries.size();
    }

    /**
     * @return the term of the last entry, or 0 if the log is empty
     */
    public long getLastTerm() {
        if (entries.isEmpty()) {
            return 0;
        }
        return entries.get(entries.size() - 1).getTerm();
    }

    /**
     * Get entries from startIndex (inclusive) through the end of the log.
     * Uses 1-based indexing.
     *
     * @return a new list of entries, or an empty list if startIndex is beyond the log
     */
    public List<LogEntryData> getEntriesFrom(long startIndex) {
        int i = (int) startIndex - 1; // convert to 0-based
        if (i < 0 || i >= entries.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(entries.subList(i, entries.size()));
    }

    /**
     * Delete all entries from the given 1-based index onward (inclusive).
     */
    public void truncateFrom(long index) {
        int i = (int) index - 1; // convert to 0-based
        if (i < 0 || i >= entries.size()) {
            return;
        }
        entries.subList(i, entries.size()).clear();
    }

    /**
     * @return the number of entries in the log
     */
    public int size() {
        return entries.size();
    }
}
