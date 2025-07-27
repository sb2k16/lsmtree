package com.lsmtree.replication;

import com.lsmtree.cluster.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a target node for replication operations.
 * Tracks replication state and statistics for a specific follower node.
 */
public class ReplicationTarget {
    
    private static final Logger logger = LoggerFactory.getLogger(ReplicationTarget.class);
    
    private final NodeInfo nodeInfo;
    private final AtomicLong lastReplicationTime;
    private final AtomicLong replicationLag;
    private final AtomicReference<ReplicationStatus> status;
    private final AtomicLong failedAttempts;
    private final AtomicLong successfulReplications;
    private final AtomicLong totalReplications;

    public ReplicationTarget(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
        this.lastReplicationTime = new AtomicLong(0);
        this.replicationLag = new AtomicLong(0);
        this.status = new AtomicReference<>(ReplicationStatus.UNKNOWN);
        this.failedAttempts = new AtomicLong(0);
        this.successfulReplications = new AtomicLong(0);
        this.totalReplications = new AtomicLong(0);
    }

    /**
     * Gets the node information.
     */
    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    /**
     * Gets the last replication time.
     */
    public long getLastReplicationTime() {
        return lastReplicationTime.get();
    }

    /**
     * Sets the last replication time.
     */
    public void setLastReplicationTime(long timestamp) {
        this.lastReplicationTime.set(timestamp);
    }

    /**
     * Gets the current replication lag in milliseconds.
     */
    public long getReplicationLag() {
        return replicationLag.get();
    }

    /**
     * Sets the replication lag.
     */
    public void setReplicationLag(long lag) {
        this.replicationLag.set(lag);
    }

    /**
     * Gets the current replication status.
     */
    public ReplicationStatus getStatus() {
        return status.get();
    }

    /**
     * Sets the replication status.
     */
    public void setStatus(ReplicationStatus status) {
        ReplicationStatus oldStatus = this.status.getAndSet(status);
        if (oldStatus != status) {
            logger.info("Replication status changed for {}: {} -> {}", 
                       nodeInfo.getNodeId(), oldStatus, status);
        }
    }

    /**
     * Gets the number of failed replication attempts.
     */
    public long getFailedAttempts() {
        return failedAttempts.get();
    }

    /**
     * Increments the failed attempts counter.
     */
    public void incrementFailedAttempts() {
        long failed = failedAttempts.incrementAndGet();
        logger.warn("Replication failed for {} (attempt {})", nodeInfo.getNodeId(), failed);
    }

    /**
     * Gets the number of successful replications.
     */
    public long getSuccessfulReplications() {
        return successfulReplications.get();
    }

    /**
     * Increments the successful replications counter.
     */
    public void incrementSuccessfulReplications() {
        long successful = successfulReplications.incrementAndGet();
        logger.debug("Replication successful for {} (total: {})", nodeInfo.getNodeId(), successful);
    }

    /**
     * Gets the total number of replication attempts.
     */
    public long getTotalReplications() {
        return totalReplications.get();
    }

    /**
     * Increments the total replications counter.
     */
    public void incrementTotalReplications() {
        totalReplications.incrementAndGet();
    }

    /**
     * Checks if this target is healthy for replication.
     */
    public boolean isHealthy() {
        return nodeInfo.isOnline() && 
               status.get() != ReplicationStatus.FAILED &&
               getReplicationLag() < 30000; // 30 seconds lag threshold
    }

    /**
     * Checks if this target is stale (too much replication lag).
     */
    public boolean isStale(long staleThresholdMs) {
        return getReplicationLag() > staleThresholdMs;
    }

    /**
     * Gets the success rate of replications.
     */
    public double getSuccessRate() {
        long total = getTotalReplications();
        if (total == 0) {
            return 0.0;
        }
        return (double) getSuccessfulReplications() / total;
    }

    /**
     * Resets the replication statistics.
     */
    public void resetStats() {
        failedAttempts.set(0);
        successfulReplications.set(0);
        totalReplications.set(0);
        logger.info("Reset replication stats for {}", nodeInfo.getNodeId());
    }

    /**
     * Gets replication statistics.
     */
    public ReplicationStats getStats() {
        return new ReplicationStats(
            nodeInfo.getNodeId(),
            getLastReplicationTime(),
            getReplicationLag(),
            getStatus(),
            getFailedAttempts(),
            getSuccessfulReplications(),
            getTotalReplications(),
            getSuccessRate()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        ReplicationTarget that = (ReplicationTarget) o;
        return Objects.equals(nodeInfo.getNodeId(), that.nodeInfo.getNodeId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeInfo.getNodeId());
    }

    @Override
    public String toString() {
        return String.format("ReplicationTarget{nodeId='%s', status=%s, lag=%dms, successRate=%.2f}",
                nodeInfo.getNodeId(), status.get(), replicationLag.get(), getSuccessRate());
    }

    /**
     * Replication status enumeration.
     */
    public enum ReplicationStatus {
        UNKNOWN,    // Initial state
        SYNCING,    // Currently synchronizing
        SYNCED,     // Successfully synchronized
        FAILED,     // Replication failed
        STALE       // Node is stale (too much lag)
    }

    /**
     * Replication statistics for a target.
     */
    public static class ReplicationStats {
        private final String nodeId;
        private final long lastReplicationTime;
        private final long replicationLag;
        private final ReplicationStatus status;
        private final long failedAttempts;
        private final long successfulReplications;
        private final long totalReplications;
        private final double successRate;

        public ReplicationStats(String nodeId, long lastReplicationTime, long replicationLag,
                               ReplicationStatus status, long failedAttempts, long successfulReplications,
                               long totalReplications, double successRate) {
            this.nodeId = nodeId;
            this.lastReplicationTime = lastReplicationTime;
            this.replicationLag = replicationLag;
            this.status = status;
            this.failedAttempts = failedAttempts;
            this.successfulReplications = successfulReplications;
            this.totalReplications = totalReplications;
            this.successRate = successRate;
        }

        // Getters
        public String getNodeId() { return nodeId; }
        public long getLastReplicationTime() { return lastReplicationTime; }
        public long getReplicationLag() { return replicationLag; }
        public ReplicationStatus getStatus() { return status; }
        public long getFailedAttempts() { return failedAttempts; }
        public long getSuccessfulReplications() { return successfulReplications; }
        public long getTotalReplications() { return totalReplications; }
        public double getSuccessRate() { return successRate; }

        @Override
        public String toString() {
            return String.format("ReplicationStats{nodeId='%s', status=%s, lag=%dms, successRate=%.2f, " +
                               "total=%d, successful=%d, failed=%d}",
                    nodeId, status, replicationLag, successRate, totalReplications, 
                    successfulReplications, failedAttempts);
        }
    }
} 