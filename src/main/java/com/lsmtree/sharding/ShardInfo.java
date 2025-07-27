package com.lsmtree.sharding;

import com.lsmtree.cluster.NodeInfo;

import java.util.List;
import java.util.Objects;

/**
 * Represents information about a shard including its leader and replica nodes.
 */
public class ShardInfo {
    
    private final int shardId;
    private final String leaderNodeId;
    private final List<String> replicaNodeIds;
    private final ShardStatus status;
    private final long lastUpdated;

    public ShardInfo(int shardId, String leaderNodeId, List<String> replicaNodeIds, ShardStatus status) {
        this.shardId = shardId;
        this.leaderNodeId = leaderNodeId;
        this.replicaNodeIds = replicaNodeIds;
        this.status = status;
        this.lastUpdated = System.currentTimeMillis();
    }

    public int getShardId() {
        return shardId;
    }

    public String getLeaderNodeId() {
        return leaderNodeId;
    }

    public List<String> getReplicaNodeIds() {
        return replicaNodeIds;
    }

    public ShardStatus getStatus() {
        return status;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public boolean isHealthy() {
        return status == ShardStatus.HEALTHY;
    }

    public boolean hasLeader() {
        return leaderNodeId != null && !leaderNodeId.isEmpty();
    }

    public boolean isNodeLeader(String nodeId) {
        return Objects.equals(leaderNodeId, nodeId);
    }

    public boolean isNodeReplica(String nodeId) {
        return replicaNodeIds.contains(nodeId);
    }

    public int getReplicaCount() {
        return replicaNodeIds.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        ShardInfo shardInfo = (ShardInfo) o;
        return shardId == shardInfo.shardId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId);
    }

    @Override
    public String toString() {
        return String.format("ShardInfo{shardId=%d, leader='%s', replicas=%s, status=%s}",
                shardId, leaderNodeId, replicaNodeIds, status);
    }

    /**
     * Shard status enumeration.
     */
    public enum ShardStatus {
        HEALTHY,        // Shard is healthy with a leader
        UNHEALTHY,      // Shard has issues (no leader, insufficient replicas)
        REBALANCING,    // Shard is being rebalanced
        FAILED          // Shard has failed completely
    }
} 