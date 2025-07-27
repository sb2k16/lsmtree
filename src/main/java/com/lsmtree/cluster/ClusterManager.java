package com.lsmtree.cluster;

import com.lsmtree.replication.ReplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages cluster operations including node discovery, health monitoring, and coordination.
 */
@Component
public class ClusterManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    
    @Value("${cluster.node.id}")
    private String localNodeId;
    
    @Value("${cluster.node.host}")
    private String localNodeHost;
    
    @Value("${cluster.node.port}")
    private int localNodePort;
    
    @Autowired
    private ClusterMembership membership;
    
    @Autowired
    private RaftConsensus consensus;
    
    @Autowired
    private ReplicationConfig replicationConfig;
    
    private final ScheduledExecutorService clusterExecutor;
    private volatile boolean running;

    public ClusterManager() {
        this.clusterExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "cluster-manager");
            t.setDaemon(true);
            return t;
        });
        this.running = false;
    }

    @PostConstruct
    public void initialize() {
        logger.info("Initializing ClusterManager for node: {}", localNodeId);
        
        // Add local node to membership
        NodeInfo localNode = new NodeInfo(localNodeId, localNodeHost, localNodePort);
        localNode.setStatus(NodeInfo.NodeStatus.ONLINE);
        membership.addNode(localNode);
        
        // Start cluster management tasks
        startClusterTasks();
        
        running = true;
        logger.info("ClusterManager initialized successfully");
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down ClusterManager");
        running = false;
        
        // Shutdown executor
        clusterExecutor.shutdown();
        try {
            if (!clusterExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                clusterExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            clusterExecutor.shutdownNow();
        }
        
        logger.info("ClusterManager shutdown complete");
    }

    /**
     * Starts cluster management tasks.
     */
    private void startClusterTasks() {
        // Heartbeat task
        clusterExecutor.scheduleAtFixedRate(
            this::sendHeartbeat,
            100, // Initial delay
            100, // Period
            TimeUnit.MILLISECONDS
        );
        
        // Health monitoring task
        clusterExecutor.scheduleAtFixedRate(
            this::monitorClusterHealth,
            1000, // Initial delay
            5000, // Period
            TimeUnit.MILLISECONDS
        );
        
        // Election timeout checking
        clusterExecutor.scheduleAtFixedRate(
            this::checkElectionTimeout,
            500, // Initial delay
            500, // Period
            TimeUnit.MILLISECONDS
        );
        
        // Stale node cleanup
        clusterExecutor.scheduleAtFixedRate(
            this::cleanupStaleNodes,
            10000, // Initial delay
            30000, // Period
            TimeUnit.SECONDS
        );
    }

    /**
     * Sends heartbeat to other nodes.
     */
    private void sendHeartbeat() {
        if (!running) return;
        
        try {
            // Update local node heartbeat
            NodeInfo localNode = membership.getLocalNode();
            if (localNode != null) {
                localNode.setLastHeartbeat(System.currentTimeMillis());
                membership.updateNode(localNode);
            }
            
            // If we're the leader, send heartbeats to followers
            if (membership.isLeader()) {
                sendLeaderHeartbeat();
            }
            
        } catch (Exception e) {
            logger.error("Error sending heartbeat", e);
        }
    }

    /**
     * Sends heartbeat as leader to followers.
     */
    private void sendLeaderHeartbeat() {
        List<NodeInfo> followers = membership.getFollowers();
        
        for (NodeInfo follower : followers) {
            try {
                // In a real implementation, this would send network messages
                // For now, we'll simulate the heartbeat
                simulateHeartbeatToFollower(follower);
            } catch (Exception e) {
                logger.warn("Failed to send heartbeat to follower: {}", follower.getNodeId(), e);
            }
        }
    }

    /**
     * Simulates sending heartbeat to a follower (placeholder implementation).
     */
    private void simulateHeartbeatToFollower(NodeInfo follower) {
        // Simulate network delay
        try {
            Thread.sleep(5 + (long) (Math.random() * 10)); // 5-15ms delay
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        
        // Update follower's heartbeat
        follower.setLastHeartbeat(System.currentTimeMillis());
        follower.setStatus(NodeInfo.NodeStatus.ONLINE);
        membership.updateNode(follower);
    }

    /**
     * Monitors cluster health and triggers failover if needed.
     */
    private void monitorClusterHealth() {
        if (!running) return;
        
        try {
            // Check if we have quorum
            if (!membership.hasQuorum()) {
                logger.warn("Cluster does not have quorum");
                return;
            }
            
            // Check if leader is healthy
            NodeInfo leader = membership.getLeader();
            if (leader != null && leader.isStale(5000)) { // 5 second timeout
                logger.warn("Leader appears to be stale: {}", leader.getNodeId());
                triggerFailover();
            }
            
            // Update replication targets
            updateReplicationTargets();
            
        } catch (Exception e) {
            logger.error("Error monitoring cluster health", e);
        }
    }

    /**
     * Checks for election timeout and starts election if needed.
     */
    private void checkElectionTimeout() {
        if (!running) return;
        
        try {
            consensus.checkElectionTimeout();
        } catch (Exception e) {
            logger.error("Error checking election timeout", e);
        }
    }

    /**
     * Cleans up stale nodes from the cluster.
     */
    private void cleanupStaleNodes() {
        if (!running) return;
        
        try {
            membership.removeStaleNodes(30000); // 30 second stale threshold
        } catch (Exception e) {
            logger.error("Error cleaning up stale nodes", e);
        }
    }

    /**
     * Triggers automatic failover.
     */
    private void triggerFailover() {
        if (!replicationConfig.isAutoFailover()) {
            logger.info("Auto failover is disabled");
            return;
        }
        
        logger.info("Triggering automatic failover");
        
        // Start election
        consensus.startElection();
    }

    /**
     * Updates replication targets based on current cluster membership.
     */
    private void updateReplicationTargets() {
        // This would be called by the replication manager
        // For now, we'll just log the current state
        List<NodeInfo> followers = membership.getFollowers();
        logger.debug("Current followers: {}", followers.size());
    }

    /**
     * Adds a node to the cluster.
     */
    public void addNode(String nodeId, String host, int port) {
        NodeInfo node = new NodeInfo(nodeId, host, port);
        node.setStatus(NodeInfo.NodeStatus.ONLINE);
        membership.addNode(node);
        logger.info("Added node to cluster: {}", node);
    }

    /**
     * Removes a node from the cluster.
     */
    public void removeNode(String nodeId) {
        membership.removeNode(nodeId);
        logger.info("Removed node from cluster: {}", nodeId);
    }

    /**
     * Gets cluster status.
     */
    public ClusterStatus getClusterStatus() {
        return new ClusterStatus(
            membership.getStats(),
            consensus.getState(),
            consensus.getCurrentTerm(),
            membership.isLeader(),
            membership.hasQuorum(),
            localNodeId
        );
    }

    /**
     * Forces a leader election.
     */
    public void forceElection() {
        logger.info("Forcing leader election");
        consensus.startElection();
    }

    /**
     * Gets the local node information.
     */
    public NodeInfo getLocalNode() {
        return membership.getLocalNode();
    }

    /**
     * Gets all nodes in the cluster.
     */
    public List<NodeInfo> getAllNodes() {
        return membership.getAllNodes();
    }

    /**
     * Gets the current leader.
     */
    public NodeInfo getLeader() {
        return membership.getLeader();
    }

    /**
     * Checks if this node is the leader.
     */
    public boolean isLeader() {
        return membership.isLeader();
    }

    /**
     * Checks if the cluster has quorum.
     */
    public boolean hasQuorum() {
        return membership.hasQuorum();
    }

    /**
     * Cluster status information.
     */
    public static class ClusterStatus {
        private final ClusterMembership.ClusterStats stats;
        private final RaftConsensus.RaftState raftState;
        private final long currentTerm;
        private final boolean isLeader;
        private final boolean hasQuorum;
        private final String localNodeId;

        public ClusterStatus(ClusterMembership.ClusterStats stats, RaftConsensus.RaftState raftState,
                           long currentTerm, boolean isLeader, boolean hasQuorum, String localNodeId) {
            this.stats = stats;
            this.raftState = raftState;
            this.currentTerm = currentTerm;
            this.isLeader = isLeader;
            this.hasQuorum = hasQuorum;
            this.localNodeId = localNodeId;
        }

        // Getters
        public ClusterMembership.ClusterStats getStats() { return stats; }
        public RaftConsensus.RaftState getRaftState() { return raftState; }
        public long getCurrentTerm() { return currentTerm; }
        public boolean isLeader() { return isLeader; }
        public boolean hasQuorum() { return hasQuorum; }
        public String getLocalNodeId() { return localNodeId; }

        @Override
        public String toString() {
            return String.format("ClusterStatus{raftState=%s, term=%d, isLeader=%s, hasQuorum=%s, localNode='%s'}",
                    raftState, currentTerm, isLeader, hasQuorum, localNodeId);
        }
    }
} 