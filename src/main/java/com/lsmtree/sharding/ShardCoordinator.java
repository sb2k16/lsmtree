package com.lsmtree.sharding;

import com.lsmtree.cluster.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Coordinates shard assignments, leader election, and request routing across the cluster.
 * Acts as a central coordinator for the sharded architecture.
 */
@Component
public class ShardCoordinator {
    
    private static final Logger logger = LoggerFactory.getLogger(ShardCoordinator.class);
    
    @Value("${sharding.num-shards:16}")
    private int numShards;
    
    @Value("${sharding.replication-factor:3}")
    private int replicationFactor;
    
    @Value("${sharding.health-check-interval-ms:5000}")
    private long healthCheckIntervalMs;
    
    private final ReentrantReadWriteLock lock;
    private final Map<Integer, ShardInfo> shardMap;
    private final Map<String, NodeInfo> nodeRegistry;
    private final Map<String, Set<Integer>> nodeToShards;
    private final ScheduledExecutorService executor;
    private volatile boolean running;

    public ShardCoordinator() {
        this.lock = new ReentrantReadWriteLock();
        this.shardMap = new ConcurrentHashMap<>();
        this.nodeRegistry = new ConcurrentHashMap<>();
        this.nodeToShards = new ConcurrentHashMap<>();
        this.executor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "shard-coordinator");
            t.setDaemon(true);
            return t;
        });
        this.running = false;
    }

    @PostConstruct
    public void initialize() {
        logger.info("Initializing ShardCoordinator with {} shards, replication factor {}", 
                   numShards, replicationFactor);
        
        // Initialize shard map
        initializeShardMap();
        
        // Start health monitoring
        startHealthMonitoring();
        
        running = true;
        logger.info("ShardCoordinator initialized successfully");
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down ShardCoordinator");
        running = false;
        
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        
        logger.info("ShardCoordinator shutdown complete");
    }

    /**
     * Registers a node with the coordinator.
     */
    public NodeRegistration registerNode(String nodeId, String host, int port) {
        lock.writeLock().lock();
        try {
            // Create node info
            NodeInfo nodeInfo = new NodeInfo(nodeId, host, port);
            nodeInfo.setStatus(NodeInfo.NodeStatus.ONLINE);
            nodeInfo.setLastHeartbeat(System.currentTimeMillis());
            
            // Register node
            nodeRegistry.put(nodeId, nodeInfo);
            nodeToShards.put(nodeId, new HashSet<>());
            
            logger.info("Registered node: {}", nodeInfo);
            
            // Assign shards to this node
            assignShardsToNode(nodeId);
            
            return new NodeRegistration(nodeId, host, port, getNodeShards(nodeId));
            
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Unregisters a node from the coordinator.
     */
    public void unregisterNode(String nodeId) {
        lock.writeLock().lock();
        try {
            NodeInfo nodeInfo = nodeRegistry.remove(nodeId);
            if (nodeInfo != null) {
                // Remove shard assignments
                Set<Integer> nodeShards = nodeToShards.remove(nodeId);
                if (nodeShards != null) {
                    for (int shardId : nodeShards) {
                        handleShardLeaderFailure(shardId, nodeId);
                    }
                }
                
                logger.info("Unregistered node: {}", nodeInfo);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the leader node for a specific shard.
     */
    public String getShardLeader(int shardId) {
        lock.readLock().lock();
        try {
            ShardInfo shardInfo = shardMap.get(shardId);
            return shardInfo != null ? shardInfo.getLeaderNodeId() : null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the shard ID for a given metric name.
     */
    public int getShardId(String metricName) {
        if (metricName == null || metricName.isEmpty()) {
            throw new IllegalArgumentException("Metric name cannot be null or empty");
        }
        return Math.abs(metricName.hashCode()) % numShards;
    }

    /**
     * Gets the leader node for a given metric.
     */
    public String getLeaderForMetric(String metricName) {
        int shardId = getShardId(metricName);
        return getShardLeader(shardId);
    }

    /**
     * Updates shard information (called when leader election completes).
     */
    public void updateShardInfo(int shardId, String leaderNodeId, List<String> replicaNodeIds) {
        lock.writeLock().lock();
        try {
            ShardInfo newInfo = new ShardInfo(shardId, leaderNodeId, replicaNodeIds, 
                                            ShardInfo.ShardStatus.HEALTHY);
            shardMap.put(shardId, newInfo);
            
            // Update node-to-shard mappings
            updateNodeShardMappings(shardId, leaderNodeId, replicaNodeIds);
            
            logger.info("Updated shard info: {}", newInfo);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Triggers leader election for a specific shard.
     */
    public void triggerShardLeaderElection(int shardId) {
        lock.readLock().lock();
        try {
            ShardInfo shardInfo = shardMap.get(shardId);
            if (shardInfo == null) {
                logger.warn("Cannot trigger election for unknown shard: {}", shardId);
                return;
            }
            
            // Find a healthy node to trigger election
            String electionNode = selectElectionNode(shardInfo.getReplicaNodeIds());
            if (electionNode != null) {
                logger.info("Triggering leader election for shard {} on node {}", shardId, electionNode);
                // In a real implementation, this would send a message to the node
                // For now, we'll just log it
            } else {
                logger.warn("No healthy nodes available for shard {} election", shardId);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the current shard map.
     */
    public Map<Integer, ShardInfo> getShardMap() {
        lock.readLock().lock();
        try {
            return new HashMap<>(shardMap);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets all registered nodes.
     */
    public List<NodeInfo> getAllNodes() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(nodeRegistry.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets shards assigned to a specific node.
     */
    public Set<Integer> getNodeShards(String nodeId) {
        lock.readLock().lock();
        try {
            Set<Integer> shards = nodeToShards.get(nodeId);
            return shards != null ? new HashSet<>(shards) : new HashSet<>();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if a node is healthy.
     */
    public boolean isNodeHealthy(String nodeId) {
        lock.readLock().lock();
        try {
            NodeInfo nodeInfo = nodeRegistry.get(nodeId);
            if (nodeInfo == null) {
                return false;
            }
            
            // Check if node is online and not stale
            return nodeInfo.isOnline() && !nodeInfo.isStale(30000); // 30 second timeout
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Initializes the shard map with empty assignments.
     */
    private void initializeShardMap() {
        lock.writeLock().lock();
        try {
            for (int i = 0; i < numShards; i++) {
                shardMap.put(i, new ShardInfo(i, null, new ArrayList<>(), 
                                            ShardInfo.ShardStatus.UNHEALTHY));
            }
            logger.info("Initialized {} shards", numShards);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Assigns shards to a newly registered node.
     */
    private void assignShardsToNode(String nodeId) {
        lock.writeLock().lock();
        try {
            // Simple round-robin assignment
            // In a real implementation, this would be more sophisticated
            List<String> allNodes = new ArrayList<>(nodeRegistry.keySet());
            int nodeIndex = allNodes.indexOf(nodeId);
            
            if (nodeIndex >= 0) {
                int shardsPerNode = numShards / allNodes.size();
                int extraShards = numShards % allNodes.size();
                
                int startShard = nodeIndex * shardsPerNode + Math.min(nodeIndex, extraShards);
                int endShard = startShard + shardsPerNode + (nodeIndex < extraShards ? 1 : 0);
                
                Set<Integer> nodeShards = nodeToShards.get(nodeId);
                for (int shardId = startShard; shardId < endShard; shardId++) {
                    nodeShards.add(shardId);
                    
                    // Update shard info
                    ShardInfo currentInfo = shardMap.get(shardId);
                    List<String> replicas = new ArrayList<>(currentInfo.getReplicaNodeIds());
                    if (!replicas.contains(nodeId)) {
                        replicas.add(nodeId);
                    }
                    
                    // Assign leader if none exists
                    String leader = currentInfo.getLeaderNodeId();
                    if (leader == null || !isNodeHealthy(leader)) {
                        leader = nodeId;
                    }
                    
                    ShardInfo newInfo = new ShardInfo(shardId, leader, replicas, 
                                                    ShardInfo.ShardStatus.HEALTHY);
                    shardMap.put(shardId, newInfo);
                }
                
                logger.info("Assigned shards {} to node {}", nodeShards, nodeId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handles shard leader failure.
     */
    private void handleShardLeaderFailure(int shardId, String failedNodeId) {
        ShardInfo shardInfo = shardMap.get(shardId);
        if (shardInfo != null && Objects.equals(shardInfo.getLeaderNodeId(), failedNodeId)) {
            logger.warn("Shard {} leader {} failed, triggering re-election", shardId, failedNodeId);
            triggerShardLeaderElection(shardId);
        }
    }

    /**
     * Updates node-to-shard mappings.
     */
    private void updateNodeShardMappings(int shardId, String leaderNodeId, List<String> replicaNodeIds) {
        // Remove old mappings for this shard
        for (Set<Integer> shards : nodeToShards.values()) {
            shards.remove(shardId);
        }
        
        // Add new mappings
        if (leaderNodeId != null) {
            nodeToShards.computeIfAbsent(leaderNodeId, k -> new HashSet<>()).add(shardId);
        }
        for (String replicaId : replicaNodeIds) {
            nodeToShards.computeIfAbsent(replicaId, k -> new HashSet<>()).add(shardId);
        }
    }

    /**
     * Selects a healthy node for election.
     */
    private String selectElectionNode(List<String> replicaNodeIds) {
        for (String nodeId : replicaNodeIds) {
            if (isNodeHealthy(nodeId)) {
                return nodeId;
            }
        }
        return null;
    }

    /**
     * Starts health monitoring.
     */
    private void startHealthMonitoring() {
        executor.scheduleAtFixedRate(
            this::monitorShardHealth,
            healthCheckIntervalMs,
            healthCheckIntervalMs,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Monitors shard health and triggers re-elections if needed.
     */
    private void monitorShardHealth() {
        if (!running) return;
        
        lock.readLock().lock();
        try {
            for (Map.Entry<Integer, ShardInfo> entry : shardMap.entrySet()) {
                int shardId = entry.getKey();
                ShardInfo shardInfo = entry.getValue();
                
                // Check if leader is healthy
                if (shardInfo.hasLeader() && !isNodeHealthy(shardInfo.getLeaderNodeId())) {
                    logger.warn("Shard {} leader {} is unhealthy, triggering re-election", 
                              shardId, shardInfo.getLeaderNodeId());
                    triggerShardLeaderElection(shardId);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Node registration information.
     */
    public static class NodeRegistration {
        private final String nodeId;
        private final String host;
        private final int port;
        private final Set<Integer> assignedShards;

        public NodeRegistration(String nodeId, String host, int port, Set<Integer> assignedShards) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
            this.assignedShards = assignedShards;
        }

        // Getters
        public String getNodeId() { return nodeId; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public Set<Integer> getAssignedShards() { return assignedShards; }

        @Override
        public String toString() {
            return String.format("NodeRegistration{nodeId='%s', host='%s', port=%d, shards=%s}",
                    nodeId, host, port, assignedShards);
        }
    }
} 