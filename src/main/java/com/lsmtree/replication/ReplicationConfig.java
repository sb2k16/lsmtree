package com.lsmtree.replication;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for replication settings.
 */
@Component
@ConfigurationProperties(prefix = "replication")
public class ReplicationConfig {
    
    /**
     * Consistency level for write operations.
     */
    private ConsistencyLevel writeConsistency = ConsistencyLevel.QUORUM;
    
    /**
     * Consistency level for read operations.
     */
    private ConsistencyLevel readConsistency = ConsistencyLevel.ONE;
    
    /**
     * Timeout for replication operations in milliseconds.
     */
    private long replicationTimeoutMs = 5000;
    
    /**
     * Maximum number of retries for failed replication operations.
     */
    private int maxRetries = 3;
    
    /**
     * Whether to use asynchronous replication.
     */
    private boolean asyncReplication = false;
    
    /**
     * Maximum replication lag in milliseconds before considering a node stale.
     */
    private long maxReplicationLagMs = 10000;
    
    /**
     * Interval for checking replication lag in milliseconds.
     */
    private long replicationLagCheckIntervalMs = 1000;
    
    /**
     * Whether to enable automatic failover.
     */
    private boolean autoFailover = true;
    
    /**
     * Timeout for failover operations in milliseconds.
     */
    private long failoverTimeoutMs = 30000;

    /**
     * Consistency levels for replication operations.
     */
    public enum ConsistencyLevel {
        /**
         * Wait for only one node to acknowledge.
         */
        ONE,
        
        /**
         * Wait for a quorum (majority) of nodes to acknowledge.
         */
        QUORUM,
        
        /**
         * Wait for all nodes to acknowledge.
         */
        ALL
    }

    // Getters and Setters
    public ConsistencyLevel getWriteConsistency() {
        return writeConsistency;
    }

    public void setWriteConsistency(ConsistencyLevel writeConsistency) {
        this.writeConsistency = writeConsistency;
    }

    public ConsistencyLevel getReadConsistency() {
        return readConsistency;
    }

    public void setReadConsistency(ConsistencyLevel readConsistency) {
        this.readConsistency = readConsistency;
    }

    public long getReplicationTimeoutMs() {
        return replicationTimeoutMs;
    }

    public void setReplicationTimeoutMs(long replicationTimeoutMs) {
        this.replicationTimeoutMs = replicationTimeoutMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public boolean isAsyncReplication() {
        return asyncReplication;
    }

    public void setAsyncReplication(boolean asyncReplication) {
        this.asyncReplication = asyncReplication;
    }

    public long getMaxReplicationLagMs() {
        return maxReplicationLagMs;
    }

    public void setMaxReplicationLagMs(long maxReplicationLagMs) {
        this.maxReplicationLagMs = maxReplicationLagMs;
    }

    public long getReplicationLagCheckIntervalMs() {
        return replicationLagCheckIntervalMs;
    }

    public void setReplicationLagCheckIntervalMs(long replicationLagCheckIntervalMs) {
        this.replicationLagCheckIntervalMs = replicationLagCheckIntervalMs;
    }

    public boolean isAutoFailover() {
        return autoFailover;
    }

    public void setAutoFailover(boolean autoFailover) {
        this.autoFailover = autoFailover;
    }

    public long getFailoverTimeoutMs() {
        return failoverTimeoutMs;
    }

    public void setFailoverTimeoutMs(long failoverTimeoutMs) {
        this.failoverTimeoutMs = failoverTimeoutMs;
    }

    @Override
    public String toString() {
        return String.format("ReplicationConfig{writeConsistency=%s, readConsistency=%s, " +
                           "timeoutMs=%d, maxRetries=%d, async=%s, maxLagMs=%d, autoFailover=%s}",
                writeConsistency, readConsistency, replicationTimeoutMs, maxRetries,
                asyncReplication, maxReplicationLagMs, autoFailover);
    }
} 