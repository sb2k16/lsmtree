package com.lsmtree.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manages cluster membership and node information.
 * Thread-safe implementation for tracking all nodes in the cluster.
 */
public class ClusterMembership {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterMembership.class);
    
    private final ReentrantReadWriteLock lock;
    private final Map<String, NodeInfo> allNodes;
    private volatile NodeInfo leader;
    private volatile long currentTerm;
    private volatile String localNodeId;

    public ClusterMembership(String localNodeId) {
        this.lock = new ReentrantReadWriteLock();
        this.allNodes = new ConcurrentHashMap<>();
        this.localNodeId = localNodeId;
        this.currentTerm = 0;
    }

    /**
     * Adds a node to the cluster.
     */
    public void addNode(NodeInfo node) {
        lock.writeLock().lock();
        try {
            allNodes.put(node.getNodeId(), node);
            logger.info("Added node to cluster: {}", node);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a node from the cluster.
     */
    public void removeNode(String nodeId) {
        lock.writeLock().lock();
        try {
            NodeInfo removed = allNodes.remove(nodeId);
            if (removed != null) {
                // If the removed node was the leader, clear leader
                if (leader != null && leader.getNodeId().equals(nodeId)) {
                    leader = null;
                    logger.warn("Leader node removed from cluster: {}", nodeId);
                }
                logger.info("Removed node from cluster: {}", removed);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates node information.
     */
    public void updateNode(NodeInfo node) {
        lock.writeLock().lock();
        try {
            NodeInfo existing = allNodes.get(node.getNodeId());
            if (existing != null) {
                allNodes.put(node.getNodeId(), node);
                
                // Update leader if this node is the leader
                if (node.isLeader()) {
                    leader = node;
                    logger.info("Updated leader node: {}", node);
                } else if (leader != null && leader.getNodeId().equals(node.getNodeId()) && !node.isLeader()) {
                    // This node was leader but is no longer
                    leader = null;
                    logger.warn("Node is no longer leader: {}", node.getNodeId());
                }
                
                logger.debug("Updated node: {}", node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Sets the leader node.
     */
    public void setLeader(NodeInfo leader) {
        lock.writeLock().lock();
        try {
            this.leader = leader;
            if (leader != null) {
                // Update the leader's role in the node map
                NodeInfo updatedLeader = new NodeInfo(
                    leader.getNodeId(), leader.getHost(), leader.getPort(),
                    NodeInfo.NodeRole.LEADER, leader.getStatus(), leader.getLastHeartbeat(), leader.getMetadata()
                );
                allNodes.put(leader.getNodeId(), updatedLeader);
                logger.info("Set leader node: {}", leader);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the current leader node.
     */
    public NodeInfo getLeader() {
        lock.readLock().lock();
        try {
            return leader;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets all nodes in the cluster.
     */
    public List<NodeInfo> getAllNodes() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(allNodes.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets all follower nodes (excluding leader).
     */
    public List<NodeInfo> getFollowers() {
        lock.readLock().lock();
        try {
            return allNodes.values().stream()
                    .filter(node -> !node.isLeader())
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets online nodes.
     */
    public List<NodeInfo> getOnlineNodes() {
        lock.readLock().lock();
        try {
            return allNodes.values().stream()
                    .filter(NodeInfo::isOnline)
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets a specific node by ID.
     */
    public NodeInfo getNode(String nodeId) {
        lock.readLock().lock();
        try {
            return allNodes.get(nodeId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the local node information.
     */
    public NodeInfo getLocalNode() {
        return getNode(localNodeId);
    }

    /**
     * Checks if this node is the leader.
     */
    public boolean isLeader() {
        lock.readLock().lock();
        try {
            return leader != null && leader.getNodeId().equals(localNodeId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the current term.
     */
    public long getCurrentTerm() {
        return currentTerm;
    }

    /**
     * Sets the current term.
     */
    public void setCurrentTerm(long term) {
        this.currentTerm = term;
        logger.info("Updated cluster term to: {}", term);
    }

    /**
     * Increments the current term.
     */
    public long incrementTerm() {
        this.currentTerm++;
        logger.info("Incremented cluster term to: {}", currentTerm);
        return currentTerm;
    }

    /**
     * Gets the quorum size (majority of nodes).
     */
    public int getQuorumSize() {
        lock.readLock().lock();
        try {
            return (allNodes.size() / 2) + 1;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if we have a quorum of online nodes.
     */
    public boolean hasQuorum() {
        lock.readLock().lock();
        try {
            int onlineCount = (int) allNodes.values().stream()
                    .filter(NodeInfo::isOnline)
                    .count();
            return onlineCount >= getQuorumSize();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets cluster statistics.
     */
    public ClusterStats getStats() {
        lock.readLock().lock();
        try {
            int totalNodes = allNodes.size();
            int onlineNodes = (int) allNodes.values().stream()
                    .filter(NodeInfo::isOnline)
                    .count();
            int followerNodes = (int) allNodes.values().stream()
                    .filter(node -> !node.isLeader())
                    .count();
            
            return new ClusterStats(totalNodes, onlineNodes, followerNodes, 
                                  leader != null ? 1 : 0, currentTerm);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Removes stale nodes based on heartbeat timeout.
     */
    public void removeStaleNodes(long staleThresholdMs) {
        lock.writeLock().lock();
        try {
            List<String> staleNodeIds = allNodes.values().stream()
                    .filter(node -> node.isStale(staleThresholdMs))
                    .map(NodeInfo::getNodeId)
                    .collect(Collectors.toList());
            
            for (String nodeId : staleNodeIds) {
                removeNode(nodeId);
                logger.warn("Removed stale node: {}", nodeId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Cluster statistics.
     */
    public static class ClusterStats {
        private final int totalNodes;
        private final int onlineNodes;
        private final int followerNodes;
        private final int leaderNodes;
        private final long currentTerm;

        public ClusterStats(int totalNodes, int onlineNodes, int followerNodes, 
                          int leaderNodes, long currentTerm) {
            this.totalNodes = totalNodes;
            this.onlineNodes = onlineNodes;
            this.followerNodes = followerNodes;
            this.leaderNodes = leaderNodes;
            this.currentTerm = currentTerm;
        }

        // Getters
        public int getTotalNodes() { return totalNodes; }
        public int getOnlineNodes() { return onlineNodes; }
        public int getFollowerNodes() { return followerNodes; }
        public int getLeaderNodes() { return leaderNodes; }
        public long getCurrentTerm() { return currentTerm; }

        @Override
        public String toString() {
            return String.format("ClusterStats{total=%d, online=%d, followers=%d, leaders=%d, term=%d}",
                    totalNodes, onlineNodes, followerNodes, leaderNodes, currentTerm);
        }
    }
} 