package com.lsmtree.api;

import com.lsmtree.model.QueryRequest;
import com.lsmtree.model.QueryResponse;
import com.lsmtree.model.TimeseriesPoint;
import com.lsmtree.replication.ReplicationManager;
import com.lsmtree.service.TimeseriesService;
import com.lsmtree.storage.ReplicatedLSMTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API controller for timeseries data operations.
 */
@RestController
@RequestMapping("/api/v1/timeseries")
public class TimeseriesController {
    
    private static final Logger logger = LoggerFactory.getLogger(TimeseriesController.class);
    
    @Autowired
    private TimeseriesService timeseriesService;

    /**
     * Write a single timeseries point.
     * POST /api/v1/timeseries/write
     */
    @PostMapping("/write")
    public ResponseEntity<Map<String, Object>> writePoint(@RequestBody TimeseriesPoint point) {
        try {
            timeseriesService.writePoint(point);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Point written successfully");
            response.put("point", point);
            
            return ResponseEntity.ok(response);
            
        } catch (IllegalStateException e) {
            logger.warn("Write operation failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(createErrorResponse("Service unavailable", e.getMessage()));
        } catch (Exception e) {
            logger.error("Error writing point", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Write failed", e.getMessage()));
        }
    }

    /**
     * Write multiple timeseries points in batch.
     * POST /api/v1/timeseries/write/batch
     */
    @PostMapping("/write/batch")
    public ResponseEntity<Map<String, Object>> writeBatch(@RequestBody List<TimeseriesPoint> points) {
        try {
            timeseriesService.writeBatch(points);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Batch write completed successfully");
            response.put("pointsCount", points.size());
            
            return ResponseEntity.ok(response);
            
        } catch (IllegalStateException e) {
            logger.warn("Batch write operation failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(createErrorResponse("Service unavailable", e.getMessage()));
        } catch (Exception e) {
            logger.error("Error writing batch", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Batch write failed", e.getMessage()));
        }
    }

    /**
     * Query timeseries data.
     * POST /api/v1/timeseries/query
     */
    @PostMapping("/query")
    public ResponseEntity<QueryResponse> query(@RequestBody QueryRequest request) {
        try {
            QueryResponse response = timeseriesService.query(request);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error executing query", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new QueryResponse(List.of(), request.getMetric(), 0));
        }
    }

    /**
     * Point lookup endpoint.
     * GET /api/v1/timeseries/point?metric=...&timestamp=...&tags=key1:val1,key2:val2
     */
    @GetMapping("/point")
    public ResponseEntity<?> pointLookup(@RequestParam String metric,
                                         @RequestParam long timestamp,
                                         @RequestParam(required = false) String tags) {
        try {
            Map<String, String> tagMap = parseTags(tags);
            TimeseriesPoint point = timeseriesService.pointLookup(metric, tagMap, timestamp);
            if (point == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("status", "not_found"));
            }
            return ResponseEntity.ok(point);
        } catch (Exception e) {
            logger.error("Error in point lookup", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    /**
     * Range query endpoint.
     * POST /api/v1/timeseries/range
     * Body: { "metric": ..., "startTime": ..., "endTime": ..., "tags": { ... } }
     */
    @PostMapping("/range")
    public ResponseEntity<?> rangeQuery(@RequestBody QueryRequest request) {
        try {
            List<TimeseriesPoint> points = timeseriesService.rangeQuery(request.getMetric(), request.getStartTime(), request.getEndTime(), request.getTags());
            return ResponseEntity.ok(points);
        } catch (Exception e) {
            logger.error("Error in range query", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    /**
     * Get cluster statistics.
     * GET /api/v1/timeseries/cluster/stats
     */
    @GetMapping("/cluster/stats")
    public ResponseEntity<ReplicatedLSMTree.ClusterStats> getClusterStats() {
        try {
            ReplicatedLSMTree.ClusterStats stats = timeseriesService.getClusterStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Error getting cluster stats", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get cluster status.
     * GET /api/v1/timeseries/cluster/status
     */
    @GetMapping("/cluster/status")
    public ResponseEntity<Map<String, Object>> getClusterStatus() {
        try {
            var status = timeseriesService.getClusterStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("isLeader", timeseriesService.isLeader());
            response.put("hasQuorum", status.hasQuorum());
            response.put("raftState", status.getRaftState());
            response.put("currentTerm", status.getCurrentTerm());
            response.put("totalNodes", status.getStats().getTotalNodes());
            response.put("onlineNodes", status.getStats().getOnlineNodes());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error getting cluster status", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Force cluster flush (leader only).
     * POST /api/v1/timeseries/cluster/flush
     */
    @PostMapping("/cluster/flush")
    public ResponseEntity<Map<String, Object>> forceClusterFlush() {
        try {
            timeseriesService.forceClusterFlush();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Cluster flush completed successfully");
            
            return ResponseEntity.ok(response);
            
        } catch (IllegalStateException e) {
            logger.warn("Cluster flush failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(createErrorResponse("Not authorized", e.getMessage()));
        } catch (Exception e) {
            logger.error("Error forcing cluster flush", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Flush failed", e.getMessage()));
        }
    }

    /**
     * Get replication statistics.
     * GET /api/v1/timeseries/replication/stats
     */
    @GetMapping("/replication/stats")
    public ResponseEntity<ReplicationManager.ReplicationStats> getReplicationStats() {
        try {
            ReplicationManager.ReplicationStats stats = timeseriesService.getReplicationStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Error getting replication stats", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Health check endpoint.
     * GET /api/v1/timeseries/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        try {
            var status = timeseriesService.getClusterStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "healthy");
            response.put("isLeader", timeseriesService.isLeader());
            response.put("hasQuorum", status.hasQuorum());
            response.put("raftState", status.getRaftState());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Health check failed", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "unhealthy");
            response.put("error", e.getMessage());
            
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
        }
    }

    /**
     * Creates an error response map.
     */
    private Map<String, Object> createErrorResponse(String error, String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "error");
        response.put("error", error);
        response.put("message", message);
        return response;
    }

    private Map<String, String> parseTags(String tags) {
        Map<String, String> map = new HashMap<>();
        if (tags == null || tags.isEmpty()) return map;
        String[] pairs = tags.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split(":");
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
            }
        }
        return map;
    }
} 