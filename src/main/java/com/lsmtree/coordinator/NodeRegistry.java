package com.lsmtree.coordinator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class NodeRegistry {
    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final Map<Integer, ShardInfo> shardMap = new ConcurrentHashMap<>();

    @Autowired
    private PersistentStore persistentStore;

    @PostConstruct
    public void init() throws IOException {
        // Load persisted data on startup
        Collection<NodeInfo> loadedNodes = persistentStore.loadNodes();
        for (NodeInfo node : loadedNodes) {
            nodes.put(node.getNodeId(), node);
        }

        Map<Integer, ShardInfo> loadedShardMap = persistentStore.loadShardMap();
        shardMap.putAll(loadedShardMap);
    }

    public void registerNode(NodeInfo nodeInfo) {
        nodes.put(nodeInfo.getNodeId(), nodeInfo);
        try {
            persistentStore.saveNodes(nodes.values());
        } catch (IOException e) {
            // Log error but don't fail the operation
            System.err.println("Failed to persist node registration: " + e.getMessage());
        }
    }

    public void unregisterNode(String nodeId) {
        nodes.remove(nodeId);
        try {
            persistentStore.saveNodes(nodes.values());
        } catch (IOException e) {
            System.err.println("Failed to persist node unregistration: " + e.getMessage());
        }
    }

    public Collection<NodeInfo> getAllNodes() {
        return nodes.values();
    }

    public void updateShardInfo(int shardId, ShardInfo info) {
        shardMap.put(shardId, info);
        try {
            persistentStore.saveShardMap(shardMap);
        } catch (IOException e) {
            System.err.println("Failed to persist shard info update: " + e.getMessage());
        }
    }

    public ShardInfo getShardInfo(int shardId) {
        return shardMap.get(shardId);
    }

    public Map<Integer, ShardInfo> getShardMap() {
        return Collections.unmodifiableMap(shardMap);
    }

    public void setNodeHealth(String nodeId, NodeInfo.HealthStatus status) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) {
            node.setHealthStatus(status);
            // Note: We don't persist health status as it's transient
        }
    }

    public Map<String, NodeInfo.HealthStatus> getNodeHealthMap() {
        Map<String, NodeInfo.HealthStatus> healthMap = new HashMap<>();
        for (NodeInfo node : nodes.values()) {
            healthMap.put(node.getNodeId(), node.getHealthStatus());
        }
        return healthMap;
    }
} 