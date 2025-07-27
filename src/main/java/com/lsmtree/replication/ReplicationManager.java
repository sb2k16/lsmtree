package com.lsmtree.replication;

import com.lsmtree.cluster.ClusterMembership;
import com.lsmtree.cluster.NodeInfo;
import com.lsmtree.model.TimeseriesPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Manages replication of writes to follower nodes.
 * Handles both synchronous and asynchronous replication with configurable consistency levels.
 */
@Component
public class ReplicationManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);
    
    @Autowired
    private ReplicationConfig config;
    
    @Autowired
    private ClusterMembership membership;
    
    private final Map<String, ReplicationTarget> replicationTargets;
    private final ExecutorService replicationExecutor;
    private final ScheduledExecutorService monitoringExecutor;
    private final AtomicLong totalReplications;
    private final AtomicLong successfulReplications;
    private final AtomicLong failedReplications;

    public ReplicationManager() {
        this.replicationTargets = new ConcurrentHashMap<>();
        this.replicationExecutor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "replication-worker");
            t.setDaemon(true);
            return t;
        });
        this.monitoringExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "replication-monitor");
            t.setDaemon(true);
            return t;
        });
        this.totalReplications = new AtomicLong(0);
        this.successfulReplications = new AtomicLong(0);
        this.failedReplications = new AtomicLong(0);
    }

    @PostConstruct
    public void initialize() {
        logger.info("Initializing ReplicationManager with config: {}", config);
        
        // Start monitoring task
        monitoringExecutor.scheduleAtFixedRate(
            this::monitorReplicationLag,
            config.getReplicationLagCheckIntervalMs(),
            config.getReplicationLagCheckIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        
        logger.info("ReplicationManager initialized successfully");
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down ReplicationManager");
        
        // Shutdown executors
        replicationExecutor.shutdown();
        monitoringExecutor.shutdown();
        
        try {
            if (!replicationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                replicationExecutor.shutdownNow();
            }
            if (!monitoringExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                monitoringExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            replicationExecutor.shutdownNow();
            monitoringExecutor.shutdownNow();
        }
        
        logger.info("ReplicationManager shutdown complete");
    }

    /**
     * Replicates a single timeseries point to followers.
     */
    public CompletableFuture<ReplicationResult> replicateWrite(TimeseriesPoint point) {
        if (point == null) {
            return CompletableFuture.completedFuture(
                new ReplicationResult(false, "Point cannot be null", 0, 0)
            );
        }
        
        List<TimeseriesPoint> points = List.of(point);
        return replicateBatch(points);
    }

    /**
     * Replicates a batch of timeseries points to followers.
     */
    public CompletableFuture<ReplicationResult> replicateBatch(List<TimeseriesPoint> points) {
        if (points == null || points.isEmpty()) {
            return CompletableFuture.completedFuture(
                new ReplicationResult(true, "No points to replicate", 0, 0)
            );
        }
        
        totalReplications.incrementAndGet();
        
        // Get healthy replication targets
        List<ReplicationTarget> healthyTargets = getHealthyTargets();
        if (healthyTargets.isEmpty()) {
            logger.warn("No healthy replication targets available");
            failedReplications.incrementAndGet();
            return CompletableFuture.completedFuture(
                new ReplicationResult(false, "No healthy targets", 0, 0)
            );
        }
        
        // Determine required acknowledgments based on consistency level
        int requiredAcks = getRequiredAcknowledgments(healthyTargets.size());
        
        if (config.isAsyncReplication()) {
            // Asynchronous replication
            return replicateAsync(points, healthyTargets, requiredAcks);
        } else {
            // Synchronous replication
            return replicateSync(points, healthyTargets, requiredAcks);
        }
    }

    /**
     * Performs synchronous replication.
     */
    private CompletableFuture<ReplicationResult> replicateSync(
            List<TimeseriesPoint> points, 
            List<ReplicationTarget> targets, 
            int requiredAcks) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<CompletableFuture<Boolean>> futures = new ArrayList<>();
                
                // Send replication requests to all targets
                for (ReplicationTarget target : targets) {
                    CompletableFuture<Boolean> future = replicateToTarget(points, target);
                    futures.add(future);
                }
                
                // Wait for required acknowledgments
                int acks = 0;
                int failures = 0;
                
                for (CompletableFuture<Boolean> future : futures) {
                    try {
                        boolean success = future.get(config.getReplicationTimeoutMs(), TimeUnit.MILLISECONDS);
                        if (success) {
                            acks++;
                        } else {
                            failures++;
                        }
                    } catch (Exception e) {
                        failures++;
                        logger.warn("Replication to target failed", e);
                    }
                }
                
                boolean success = acks >= requiredAcks;
                if (success) {
                    successfulReplications.incrementAndGet();
                    logger.debug("Synchronous replication successful: {}/{} acks", acks, targets.size());
                } else {
                    failedReplications.incrementAndGet();
                    logger.warn("Synchronous replication failed: {}/{} acks required, got {}", 
                              requiredAcks, targets.size(), acks);
                }
                
                return new ReplicationResult(success, 
                    String.format("Replicated to %d/%d nodes", acks, targets.size()), 
                    acks, failures);
                
            } catch (Exception e) {
                failedReplications.incrementAndGet();
                logger.error("Synchronous replication failed", e);
                return new ReplicationResult(false, e.getMessage(), 0, targets.size());
            }
        }, replicationExecutor);
    }

    /**
     * Performs asynchronous replication.
     */
    private CompletableFuture<ReplicationResult> replicateAsync(
            List<TimeseriesPoint> points, 
            List<ReplicationTarget> targets, 
            int requiredAcks) {
        
        // Fire and forget - don't wait for acknowledgments
        for (ReplicationTarget target : targets) {
            replicateToTarget(points, target)
                .thenAccept(success -> {
                    if (success) {
                        successfulReplications.incrementAndGet();
                    } else {
                        failedReplications.incrementAndGet();
                    }
                })
                .exceptionally(throwable -> {
                    failedReplications.incrementAndGet();
                    logger.warn("Async replication to {} failed", target.getNodeInfo().getNodeId(), throwable);
                    return null;
                });
        }
        
        return CompletableFuture.completedFuture(
            new ReplicationResult(true, "Async replication initiated", targets.size(), 0)
        );
    }

    /**
     * Replicates data to a specific target.
     */
    private CompletableFuture<Boolean> replicateToTarget(List<TimeseriesPoint> points, ReplicationTarget target) {
        return CompletableFuture.supplyAsync(() -> {
            target.incrementTotalReplications();
            target.setStatus(ReplicationTarget.ReplicationStatus.SYNCING);
            
            try {
                // Simulate replication to target (in real implementation, this would be network call)
                boolean success = simulateReplicationToTarget(points, target);
                
                if (success) {
                    target.incrementSuccessfulReplications();
                    target.setStatus(ReplicationTarget.ReplicationStatus.SYNCED);
                    target.setLastReplicationTime(System.currentTimeMillis());
                    target.setReplicationLag(0);
                    logger.debug("Replication to {} successful", target.getNodeInfo().getNodeId());
                } else {
                    target.incrementFailedAttempts();
                    target.setStatus(ReplicationTarget.ReplicationStatus.FAILED);
                    logger.warn("Replication to {} failed", target.getNodeInfo().getNodeId());
                }
                
                return success;
                
            } catch (Exception e) {
                target.incrementFailedAttempts();
                target.setStatus(ReplicationTarget.ReplicationStatus.FAILED);
                logger.error("Replication to {} failed with exception", target.getNodeInfo().getNodeId(), e);
                return false;
            }
        }, replicationExecutor);
    }

    /**
     * Simulates replication to a target (placeholder implementation).
     */
    private boolean simulateReplicationToTarget(List<TimeseriesPoint> points, ReplicationTarget target) {
        // Simulate network delay and potential failure
        try {
            Thread.sleep(10 + (long) (Math.random() * 50)); // 10-60ms delay
            
            // Simulate occasional failures (5% failure rate)
            if (Math.random() < 0.05) {
                return false;
            }
            
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Gets healthy replication targets.
     */
    private List<ReplicationTarget> getHealthyTargets() {
        return replicationTargets.values().stream()
                .filter(ReplicationTarget::isHealthy)
                .collect(Collectors.toList());
    }

    /**
     * Calculates required acknowledgments based on consistency level.
     */
    private int getRequiredAcknowledgments(int totalTargets) {
        switch (config.getWriteConsistency()) {
            case ONE:
                return 1;
            case QUORUM:
                return (totalTargets / 2) + 1;
            case ALL:
                return totalTargets;
            default:
                return 1;
        }
    }

    /**
     * Updates replication targets based on cluster membership.
     */
    public void updateReplicationTargets() {
        List<NodeInfo> followers = membership.getFollowers();
        
        // Add new targets
        for (NodeInfo follower : followers) {
            if (!replicationTargets.containsKey(follower.getNodeId())) {
                ReplicationTarget target = new ReplicationTarget(follower);
                replicationTargets.put(follower.getNodeId(), target);
                logger.info("Added replication target: {}", follower.getNodeId());
            }
        }
        
        // Remove targets that are no longer in cluster
        List<String> targetIds = new ArrayList<>(replicationTargets.keySet());
        for (String targetId : targetIds) {
            if (followers.stream().noneMatch(f -> f.getNodeId().equals(targetId))) {
                replicationTargets.remove(targetId);
                logger.info("Removed replication target: {}", targetId);
            }
        }
    }

    /**
     * Monitors replication lag for all targets.
     */
    private void monitorReplicationLag() {
        try {
            for (ReplicationTarget target : replicationTargets.values()) {
                long lag = System.currentTimeMillis() - target.getLastReplicationTime();
                target.setReplicationLag(lag);
                
                if (target.isStale(config.getMaxReplicationLagMs())) {
                    target.setStatus(ReplicationTarget.ReplicationStatus.STALE);
                    logger.warn("Replication target {} is stale (lag: {}ms)", 
                              target.getNodeInfo().getNodeId(), lag);
                }
            }
        } catch (Exception e) {
            logger.error("Error monitoring replication lag", e);
        }
    }

    /**
     * Gets replication statistics.
     */
    public ReplicationStats getStats() {
        List<ReplicationTarget.ReplicationStats> targetStats = replicationTargets.values().stream()
                .map(ReplicationTarget::getStats)
                .collect(Collectors.toList());
        
        return new ReplicationStats(
            totalReplications.get(),
            successfulReplications.get(),
            failedReplications.get(),
            targetStats
        );
    }

    /**
     * Replication result.
     */
    public static class ReplicationResult {
        private final boolean success;
        private final String message;
        private final int acknowledgments;
        private final int failures;

        public ReplicationResult(boolean success, String message, int acknowledgments, int failures) {
            this.success = success;
            this.message = message;
            this.acknowledgments = acknowledgments;
            this.failures = failures;
        }

        // Getters
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public int getAcknowledgments() { return acknowledgments; }
        public int getFailures() { return failures; }

        @Override
        public String toString() {
            return String.format("ReplicationResult{success=%s, message='%s', acks=%d, failures=%d}",
                    success, message, acknowledgments, failures);
        }
    }

    /**
     * Overall replication statistics.
     */
    public static class ReplicationStats {
        private final long totalReplications;
        private final long successfulReplications;
        private final long failedReplications;
        private final List<ReplicationTarget.ReplicationStats> targetStats;

        public ReplicationStats(long totalReplications, long successfulReplications, 
                              long failedReplications, List<ReplicationTarget.ReplicationStats> targetStats) {
            this.totalReplications = totalReplications;
            this.successfulReplications = successfulReplications;
            this.failedReplications = failedReplications;
            this.targetStats = targetStats;
        }

        // Getters
        public long getTotalReplications() { return totalReplications; }
        public long getSuccessfulReplications() { return successfulReplications; }
        public long getFailedReplications() { return failedReplications; }
        public List<ReplicationTarget.ReplicationStats> getTargetStats() { return targetStats; }

        public double getSuccessRate() {
            if (totalReplications == 0) return 0.0;
            return (double) successfulReplications / totalReplications;
        }

        @Override
        public String toString() {
            return String.format("ReplicationStats{total=%d, successful=%d, failed=%d, successRate=%.2f}",
                    totalReplications, successfulReplications, failedReplications, getSuccessRate());
        }
    }
} 