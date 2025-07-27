package com.lsmtree.sharding;

import com.lsmtree.cluster.ClusterMembership;
import com.lsmtree.model.TimeseriesPoint;
import com.lsmtree.storage.ReplicatedLSMTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages shard-aware LSM trees and coordinates with the shard coordinator.
 * Each node manages multiple LSM trees, one per assigned shard.
 */
@Component
public class ShardManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ShardManager.class);
    
    @Value("${cluster.node.id}")
    private String localNodeId;
    
    @Value("${cluster.node.host}")
    private String localNodeHost;
    
    @Value("${cluster.node.port}")
    private int localNodePort;
    
    @Autowired
    private ShardCoordinator coordinator;
    
    @Autowired
    private ClusterMembership clusterMembership;
    
    private final Map<Integer, ShardRaftConsensus> shardConsensus;
    private final Map<Integer, ReplicatedLSMTree> shardTrees;
    private final ScheduledExecutorService executor;
    private volatile boolean running;

    public ShardManager() {
        this.shardConsensus = new ConcurrentHashMap<>();
        this.shardTrees = new ConcurrentHashMap<>();
        this.executor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "shard-manager");
            t.setDaemon(true);
            return t;
        });
        this.running = false;
    }

    @PostConstruct
    public void initialize() {
        logger.info("Initializing ShardManager for node: {}", localNodeId);
        
        // Register with coordinator
        registerWithCoordinator();
        
        // Start shard management tasks
        startShardManagement();
        
        running = true;
        logger.info("ShardManager initialized successfully");
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down ShardManager");
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
        
        logger.info("ShardManager shutdown complete");
    }

    /**
     * Registers this node with the coordinator.
     */
    private void registerWithCoordinator() {
        try {
            ShardCoordinator.NodeRegistration registration = 
                coordinator.registerNode(localNodeId, localNodeHost, localNodePort);
            
            logger.info("Registered with coordinator: {}", registration);
            
            // Initialize shards assigned to this node
            initializeAssignedShards(registration.getAssignedShards());
            
        } catch (Exception e) {
            logger.error("Failed to register with coordinator", e);
        }
    }

    /**
     * Initializes shards assigned to this node.
     */
    private void initializeAssignedShards(Set<Integer> assignedShards) {
        for (int shardId : assignedShards) {
            try {
                initializeShard(shardId);
                logger.info("Initialized shard: {}", shardId);
            } catch (Exception e) {
                logger.error("Failed to initialize shard: {}", shardId, e);
            }
        }
    }

    /**
     * Initializes a specific shard.
     */
    private void initializeShard(int shardId) {
        // Create shard-aware Raft consensus
        ShardRaftConsensus consensus = new ShardRaftConsensus(shardId, localNodeId, clusterMembership);
        shardConsensus.put(shardId, consensus);
        
        // Create shard-specific LSM tree
        ReplicatedLSMTree tree = new ReplicatedLSMTree();
        shardTrees.put(shardId, tree);
        
        // Start consensus
        consensus.start();
        
        logger.info("Initialized shard {} with consensus and LSM tree", shardId);
    }

    /**
     * Writes a timeseries point to the appropriate shard.
     */
    public void writePoint(TimeseriesPoint point) throws IOException {
        if (point == null) {
            throw new IllegalArgumentException("Point cannot be null");
        }
        
        int shardId = coordinator.getShardId(point.getMetric());
        ReplicatedLSMTree tree = shardTrees.get(shardId);
        
        if (tree == null) {
            throw new IllegalStateException("No LSM tree found for shard: " + shardId);
        }
        
        // Check if this node is the leader for this shard
        ShardRaftConsensus consensus = shardConsensus.get(shardId);
        if (consensus == null || !consensus.isShardLeader()) {
            throw new IllegalStateException("This node is not the leader for shard: " + shardId);
        }
        
        // Write to the shard's LSM tree
        tree.write(point);
        logger.debug("Wrote point to shard {}: {}", shardId, point);
    }

    /**
     * Writes multiple timeseries points to their respective shards.
     */
    public void writeBatch(List<TimeseriesPoint> points) throws IOException {
        if (points == null || points.isEmpty()) {
            return;
        }
        
        // Group points by shard
        Map<Integer, List<TimeseriesPoint>> pointsByShard = new HashMap<>();
        
        for (TimeseriesPoint point : points) {
            int shardId = coordinator.getShardId(point.getMetric());
            pointsByShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(point);
        }
        
        // Write each group to its respective shard
        for (Map.Entry<Integer, List<TimeseriesPoint>> entry : pointsByShard.entrySet()) {
            int shardId = entry.getKey();
            List<TimeseriesPoint> shardPoints = entry.getValue();
            
            ReplicatedLSMTree tree = shardTrees.get(shardId);
            if (tree == null) {
                throw new IllegalStateException("No LSM tree found for shard: " + shardId);
            }
            
            // Check if this node is the leader for this shard
            ShardRaftConsensus consensus = shardConsensus.get(shardId);
            if (consensus == null || !consensus.isShardLeader()) {
                throw new IllegalStateException("This node is not the leader for shard: " + shardId);
            }
            
            // Write batch to the shard's LSM tree
            tree.writeBatch(shardPoints);
            logger.debug("Wrote {} points to shard {}", shardPoints.size(), shardId);
        }
    }

    /**
     * Reads timeseries points from the appropriate shard.
     */
    public List<TimeseriesPoint> readRange(String metric, long startTime, long endTime) {
        if (metric == null || metric.isEmpty()) {
            throw new IllegalArgumentException("Metric cannot be null or empty");
        }
        
        int shardId = coordinator.getShardId(metric);
        ReplicatedLSMTree tree = shardTrees.get(shardId);
        
        if (tree == null) {
            logger.warn("No LSM tree found for shard: {}", shardId);
            return new ArrayList<>();
        }
        
        return tree.readRange(metric, startTime, endTime);
    }

    /**
     * Reads timeseries points from all shards.
     */
    public List<TimeseriesPoint> readRange(long startTime, long endTime) {
        List<TimeseriesPoint> allPoints = new ArrayList<>();
        
        for (ReplicatedLSMTree tree : shardTrees.values()) {
            List<TimeseriesPoint> shardPoints = tree.readRange(startTime, endTime);
            allPoints.addAll(shardPoints);
        }
        
        return allPoints;
    }

    /**
     * Gets the shard ID for a given metric.
     */
    public int getShardId(String metric) {
        return coordinator.getShardId(metric);
    }

    /**
     * Gets the leader node for a given metric.
     */
    public String getLeaderForMetric(String metric) {
        return coordinator.getLeaderForMetric(metric);
    }

    /**
     * Checks if this node is the leader for a specific shard.
     */
    public boolean isShardLeader(int shardId) {
        ShardRaftConsensus consensus = shardConsensus.get(shardId);
        return consensus != null && consensus.isShardLeader();
    }

    /**
     * Gets all shards assigned to this node.
     */
    public Set<Integer> getAssignedShards() {
        return new HashSet<>(shardTrees.keySet());
    }

    /**
     * Gets shard statistics.
     */
    public Map<Integer, ShardStats> getShardStats() {
        Map<Integer, ShardStats> stats = new HashMap<>();
        
        for (Map.Entry<Integer, ReplicatedLSMTree> entry : shardTrees.entrySet()) {
            int shardId = entry.getKey();
            ReplicatedLSMTree tree = entry.getValue();
            ShardRaftConsensus consensus = shardConsensus.get(shardId);
            
            ShardStats shardStats = new ShardStats(
                shardId,
                consensus != null && consensus.isShardLeader(),
                consensus != null ? consensus.getState() : null,
                consensus != null ? consensus.getCurrentTerm() : 0,
                tree.getClusterStats()
            );
            
            stats.put(shardId, shardStats);
        }
        
        return stats;
    }

    /**
     * Starts shard management tasks.
     */
    private void startShardManagement() {
        // Monitor shard health
        executor.scheduleAtFixedRate(
            this::monitorShardHealth,
            5000, // 5 seconds
            10000, // 10 seconds
            TimeUnit.MILLISECONDS
        );
        
        // Update coordinator with shard status
        executor.scheduleAtFixedRate(
            this::updateCoordinatorStatus,
            10000, // 10 seconds
            30000, // 30 seconds
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Monitors health of assigned shards.
     */
    private void monitorShardHealth() {
        if (!running) return;
        
        for (Map.Entry<Integer, ShardRaftConsensus> entry : shardConsensus.entrySet()) {
            int shardId = entry.getKey();
            ShardRaftConsensus consensus = entry.getValue();
            
            // Check election timeout
            consensus.checkElectionTimeout();
            
            // Log shard status
            if (consensus.isShardLeader()) {
                logger.debug("Shard {}: LEADER (term {})", shardId, consensus.getCurrentTerm());
            } else {
                logger.debug("Shard {}: {} (term {})", shardId, consensus.getState(), consensus.getCurrentTerm());
            }
        }
    }

    /**
     * Updates coordinator with current shard status.
     */
    private void updateCoordinatorStatus() {
        if (!running) return;
        
        try {
            // Get current shard assignments
            Set<Integer> assignedShards = getAssignedShards();
            
            // Update coordinator with shard information
            for (int shardId : assignedShards) {
                ShardRaftConsensus consensus = shardConsensus.get(shardId);
                if (consensus != null && consensus.isShardLeader()) {
                    // This node is the leader for this shard
                    // In a real implementation, we would send this info to the coordinator
                    logger.debug("Node {} is leader for shard {}", localNodeId, shardId);
                }
            }
        } catch (Exception e) {
            logger.error("Error updating coordinator status", e);
        }
    }

    /**
     * Shard statistics.
     */
    public static class ShardStats {
        private final int shardId;
        private final boolean isLeader;
        private final Object raftState; // RaftConsensus.RaftState
        private final long currentTerm;
        private final ReplicatedLSMTree.ClusterStats clusterStats;

        public ShardStats(int shardId, boolean isLeader, Object raftState, 
                         long currentTerm, ReplicatedLSMTree.ClusterStats clusterStats) {
            this.shardId = shardId;
            this.isLeader = isLeader;
            this.raftState = raftState;
            this.currentTerm = currentTerm;
            this.clusterStats = clusterStats;
        }

        // Getters
        public int getShardId() { return shardId; }
        public boolean isLeader() { return isLeader; }
        public Object getRaftState() { return raftState; }
        public long getCurrentTerm() { return currentTerm; }
        public ReplicatedLSMTree.ClusterStats getClusterStats() { return clusterStats; }

        @Override
        public String toString() {
            return String.format("ShardStats{shardId=%d, isLeader=%s, raftState=%s, term=%d}",
                    shardId, isLeader, raftState, currentTerm);
        }
    }
} 