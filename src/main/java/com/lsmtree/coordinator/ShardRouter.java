package com.lsmtree.coordinator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ShardRouter {
    @Autowired
    private NodeRegistry nodeRegistry;

    public int getShardId(String metric) {
        // Example: hash the metric name
        return Math.abs(metric.hashCode()) % 16; // Assume 16 shards for now
    }

    public NodeInfo getLeaderNode(int shardId) {
        ShardInfo info = nodeRegistry.getShardInfo(shardId);
        if (info == null) return null;
        String leaderId = info.getLeaderNodeId();
        return nodeRegistry.getAllNodes().stream()
                .filter(n -> n.getNodeId().equals(leaderId))
                .filter(n -> n.getHealthStatus() == NodeInfo.HealthStatus.HEALTHY)
                .findFirst().orElse(null);
    }

    public NodeInfo getHealthyNodeForShard(int shardId) {
        ShardInfo info = nodeRegistry.getShardInfo(shardId);
        if (info == null) return null;
        // Try to find a healthy replica
        for (String nodeId : info.getReplicaNodeIds()) {
            NodeInfo node = nodeRegistry.getAllNodes().stream()
                    .filter(n -> n.getNodeId().equals(nodeId))
                    .filter(n -> n.getHealthStatus() == NodeInfo.HealthStatus.HEALTHY)
                    .findFirst().orElse(null);
            if (node != null) return node;
        }
        // Fallback: try leader if healthy
        return getLeaderNode(shardId);
    }
} 