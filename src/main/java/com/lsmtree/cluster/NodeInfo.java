package com.lsmtree.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Represents information about a node in the cluster.
 */
public class NodeInfo {
    
    public enum NodeRole {
        LEADER, FOLLOWER, CANDIDATE
    }
    
    public enum NodeStatus {
        ONLINE, OFFLINE, SYNCHRONIZING, UNKNOWN
    }
    
    private final String nodeId;
    private final String host;
    private final int port;
    private volatile NodeRole role;
    private volatile NodeStatus status;
    private volatile long lastHeartbeat;
    private final Map<String, String> metadata;

    @JsonCreator
    public NodeInfo(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("host") String host,
            @JsonProperty("port") int port,
            @JsonProperty("role") NodeRole role,
            @JsonProperty("status") NodeStatus status,
            @JsonProperty("lastHeartbeat") long lastHeartbeat,
            @JsonProperty("metadata") Map<String, String> metadata) {
        this.nodeId = Objects.requireNonNull(nodeId, "Node ID cannot be null");
        this.host = Objects.requireNonNull(host, "Host cannot be null");
        this.port = port;
        this.role = role != null ? role : NodeRole.FOLLOWER;
        this.status = status != null ? status : NodeStatus.UNKNOWN;
        this.lastHeartbeat = lastHeartbeat;
        this.metadata = metadata;
    }

    public NodeInfo(String nodeId, String host, int port) {
        this(nodeId, host, port, NodeRole.FOLLOWER, NodeStatus.UNKNOWN, 0, null);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public NodeRole getRole() {
        return role;
    }

    public void setRole(NodeRole role) {
        this.role = role;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setStatus(NodeStatus status) {
        this.status = status;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public String getAddress() {
        return host + ":" + port;
    }

    public boolean isLeader() {
        return role == NodeRole.LEADER;
    }

    public boolean isOnline() {
        return status == NodeStatus.ONLINE;
    }

    public boolean isStale(long staleThresholdMs) {
        return System.currentTimeMillis() - lastHeartbeat > staleThresholdMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        NodeInfo nodeInfo = (NodeInfo) o;
        return Objects.equals(nodeId, nodeInfo.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return String.format("NodeInfo{nodeId='%s', host='%s', port=%d, role=%s, status=%s, lastHeartbeat=%d}",
                nodeId, host, port, role, status, lastHeartbeat);
    }
} 