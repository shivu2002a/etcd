package com.auditraft.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents the set of voting members in the Raft cluster.
 * Each member is identified by a serverId mapped to a "host:port" address.
 */
public class ClusterConfiguration {

    private final Map<String, String> members; // serverId → "host:port"

    public ClusterConfiguration(Map<String, String> members) {
        this.members = new HashMap<>(members);
    }

    /** Copy constructor. */
    public ClusterConfiguration(ClusterConfiguration other) {
        this.members = new HashMap<>(other.members);
    }

    public boolean containsMember(String serverId) {
        return members.containsKey(serverId);
    }

    public void addMember(String serverId, String address) {
        members.put(serverId, address);
    }

    public void removeMember(String serverId) {
        members.remove(serverId);
    }

    public Set<String> getMemberIds() {
        return Collections.unmodifiableSet(members.keySet());
    }

    public String getAddress(String serverId) {
        return members.get(serverId);
    }

    public Map<String, String> getMembers() {
        return new HashMap<>(members);
    }

    public int size() {
        return members.size();
    }

    /** Returns the number of nodes needed for a majority: size/2 + 1. */
    public int majoritySize() {
        return size() / 2 + 1;
    }

    /**
     * Serializes this configuration to a simple JSON string.
     * Format: {"members":{"node-1":"localhost:5001","node-2":"localhost:5002"}}
     */
    public String serialize() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"members\":{");
        boolean first = true;
        for (Map.Entry<String, String> entry : members.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(escapeJson(entry.getKey())).append("\"");
            sb.append(":");
            sb.append("\"").append(escapeJson(entry.getValue())).append("\"");
            first = false;
        }
        sb.append("}}");
        return sb.toString();
    }

    /**
     * Deserializes a JSON string into a ClusterConfiguration.
     * Expects format: {"members":{"node-1":"localhost:5001","node-2":"localhost:5002"}}
     */
    public static ClusterConfiguration deserialize(String json) {
        Map<String, String> parsed = new HashMap<>();
        if (json == null || json.isEmpty()) {
            return new ClusterConfiguration(parsed);
        }

        // Strip outer braces and the "members": wrapper
        String trimmed = json.trim();
        // Remove leading {"members":{ and trailing }}
        int innerStart = trimmed.indexOf("{", trimmed.indexOf("{") + 1);
        int innerEnd = trimmed.lastIndexOf("}");
        if (innerStart < 0 || innerEnd < 0 || innerEnd <= innerStart) {
            return new ClusterConfiguration(parsed);
        }
        String inner = trimmed.substring(innerStart + 1, innerEnd).trim();
        if (inner.isEmpty()) {
            return new ClusterConfiguration(parsed);
        }

        // Split on commas that are between entries (not inside quotes)
        // Simple approach: split by "," then parse key:value pairs
        String[] pairs = splitEntries(inner);
        for (String pair : pairs) {
            pair = pair.trim();
            if (pair.isEmpty()) {
                continue;
            }
            // Each pair is "key":"value"
            int colonIdx = pair.indexOf(":");
            if (colonIdx < 0) {
                continue;
            }
            String key = stripQuotes(pair.substring(0, colonIdx).trim());
            String value = stripQuotes(pair.substring(colonIdx + 1).trim());
            parsed.put(key, value);
        }
        return new ClusterConfiguration(parsed);
    }

    private static String[] splitEntries(String inner) {
        // Split on commas that separate "key":"value" pairs
        // Since our keys/values shouldn't contain commas in normal usage,
        // a simple split works here.
        return inner.split(",");
    }

    private static String stripQuotes(String s) {
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
