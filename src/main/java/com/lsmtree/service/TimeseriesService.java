package com.lsmtree.service;

import com.lsmtree.cluster.ClusterManager;
import com.lsmtree.model.QueryRequest;
import com.lsmtree.model.QueryResponse;
import com.lsmtree.model.TimeseriesPoint;
import com.lsmtree.replication.ReplicationManager;
import com.lsmtree.sharding.ShardManager;
import com.lsmtree.storage.ReplicatedLSMTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service layer for timeseries operations.
 * Handles business logic and coordinates with the LSM tree storage.
 */
@Service
public class TimeseriesService {
    
    private static final Logger logger = LoggerFactory.getLogger(TimeseriesService.class);
    
    @Autowired
    private ReplicatedLSMTree lsmTree;
    
    @Autowired
    private ClusterManager clusterManager;
    
    @Autowired
    private ShardManager shardManager;

    /**
     * Writes a single timeseries point with replication.
     */
    public void writePoint(TimeseriesPoint point) throws IOException {
        if (point == null) {
            throw new IllegalArgumentException("Point cannot be null");
        }
        
        // Check if we have quorum before writing
        if (!clusterManager.hasQuorum()) {
            throw new IllegalStateException("No quorum available for write operation");
        }
        
        lsmTree.write(point);
        logger.debug("Wrote point with replication: {}", point);
    }

    /**
     * Writes multiple timeseries points with replication.
     */
    public void writeBatch(List<TimeseriesPoint> points) throws IOException {
        if (points == null || points.isEmpty()) {
            return;
        }
        
        // Check if we have quorum before writing
        if (!clusterManager.hasQuorum()) {
            throw new IllegalStateException("No quorum available for write operation");
        }
        
        lsmTree.writeBatch(points);
        logger.debug("Wrote batch with replication: {} points", points.size());
    }

    /**
     * Queries timeseries data with replication awareness.
     */
    public QueryResponse query(QueryRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Query request cannot be null");
        }
        
        try {
            List<TimeseriesPoint> points;
            
            if (request.getMetric() != null && !request.getMetric().isEmpty()) {
                points = lsmTree.readRange(request.getMetric(), request.getStartTime(), request.getEndTime());
            } else {
                points = lsmTree.readRange(request.getStartTime(), request.getEndTime());
            }
            
            long queryTime = System.currentTimeMillis() - System.currentTimeMillis(); // Placeholder
            return new QueryResponse(points, request.getMetric(), queryTime);
            
        } catch (Exception e) {
            logger.error("Error executing query", e);
            throw new RuntimeException("Query failed: " + e.getMessage(), e);
        }
    }

    /**
     * Queries timeseries data for a specific metric and time range.
     */
    public QueryResponse query(String metric, long startTime, long endTime) {
        QueryRequest request = new QueryRequest(metric, startTime, endTime);
        return query(request);
    }

    /**
     * Queries timeseries data for any metric within a time range.
     */
    public List<TimeseriesPoint> queryRange(long startTime, long endTime) {
        long startQueryTime = System.currentTimeMillis();
        
        try {
            List<TimeseriesPoint> points = lsmTree.readRange(startTime, endTime);
            
            long queryTime = System.currentTimeMillis() - startQueryTime;
            logger.debug("Range query completed in {}ms, returned {} points", queryTime, points.size());
            
            return points;
        } catch (Exception e) {
            logger.error("Range query failed", e);
            throw new RuntimeException("Range query failed", e);
        }
    }

    /**
     * Point lookup for a specific metric, tags, and timestamp.
     */
    public TimeseriesPoint pointLookup(String metric, Map<String, String> tags, long timestamp) throws IOException {
        return lsmTree.pointLookup(metric, tags, timestamp);
    }

    /**
     * Range query for a metric, time range, and optional tags.
     */
    public List<TimeseriesPoint> rangeQuery(String metric, long startTime, long endTime, Map<String, String> tags) {
        List<TimeseriesPoint> all = lsmTree.readRange(metric, startTime, endTime);
        if (tags == null || tags.isEmpty()) return all;
        return all.stream().filter(p -> matchesTags(p, tags)).collect(Collectors.toList());
    }

    /**
     * Gets cluster statistics.
     */
    public ReplicatedLSMTree.ClusterStats getClusterStats() {
        return lsmTree.getClusterStats();
    }

    /**
     * Gets shard statistics.
     */
    public Map<Integer, ShardManager.ShardStats> getShardStats() {
        return shardManager.getShardStats();
    }

    /**
     * Checks if this node is the leader.
     */
    public boolean isLeader() {
        return clusterManager.isLeader();
    }

    /**
     * Gets cluster health information.
     */
    public ClusterManager.ClusterStatus getClusterStatus() {
        return clusterManager.getClusterStatus();
    }

    /**
     * Forces a cluster-wide flush.
     */
    public void forceClusterFlush() throws IOException {
        if (!isLeader()) {
            throw new IllegalStateException("Only leader can initiate cluster flush");
        }
        
        lsmTree.forceClusterFlush();
        logger.info("Cluster-wide flush completed");
    }

    /**
     * Gets replication statistics.
     */
    public ReplicationManager.ReplicationStats getReplicationStats() {
        return lsmTree.getReplicationStats();
    }

    /**
     * Checks if the service is healthy.
     */
    public boolean isHealthy() {
        try {
            // Basic health check - try to get stats
            lsmTree.getStats();
            return true;
        } catch (Exception e) {
            logger.error("Health check failed", e);
            return false;
        }
    }

    /**
     * Checks if a timeseries point matches the specified tags.
     */
    private boolean matchesTags(TimeseriesPoint point, java.util.Map<String, String> requiredTags) {
        java.util.Map<String, String> pointTags = point.getTags();
        
        for (java.util.Map.Entry<String, String> requiredTag : requiredTags.entrySet()) {
            String pointTagValue = pointTags.get(requiredTag.getKey());
            if (!java.util.Objects.equals(requiredTag.getValue(), pointTagValue)) {
                return false;
            }
        }
        
        return true;
    }
} 