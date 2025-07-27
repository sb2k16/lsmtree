package com.lsmtree.api;

import com.lsmtree.sharding.ShardManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/shard")
public class ShardController {
    
    private static final Logger logger = LoggerFactory.getLogger(ShardController.class);
    
    @Autowired
    private ShardManager shardManager;

    /**
     * Get shard statistics for this node.
     * GET /api/v1/shard/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<Integer, ShardManager.ShardStats>> getShardStats() {
        try {
            Map<Integer, ShardManager.ShardStats> stats = shardManager.getShardStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Failed to get shard stats", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get statistics for a specific shard.
     * GET /api/v1/shard/{shardId}/stats
     */
    @GetMapping("/{shardId}/stats")
    public ResponseEntity<ShardManager.ShardStats> getShardStats(@PathVariable int shardId) {
        try {
            Map<Integer, ShardManager.ShardStats> allStats = shardManager.getShardStats();
            ShardManager.ShardStats stats = allStats.get(shardId);
            
            if (stats == null) {
                return ResponseEntity.notFound().build();
            }
            
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Failed to get stats for shard: {}", shardId, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get all shards assigned to this node.
     * GET /api/v1/shard/assigned
     */
    @GetMapping("/assigned")
    public ResponseEntity<Map<String, Object>> getAssignedShards() {
        try {
            var assignedShards = shardManager.getAssignedShards();
            
            Map<String, Object> response = new HashMap<>();
            response.put("assignedShards", assignedShards);
            response.put("totalAssignedShards", assignedShards.size());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get assigned shards", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Check if this node is the leader for a specific shard.
     * GET /api/v1/shard/{shardId}/leader
     */
    @GetMapping("/{shardId}/leader")
    public ResponseEntity<Map<String, Object>> isShardLeader(@PathVariable int shardId) {
        try {
            boolean isLeader = shardManager.isShardLeader(shardId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("shardId", shardId);
            response.put("isLeader", isLeader);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to check leader status for shard: {}", shardId, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get the shard ID for a metric.
     * GET /api/v1/shard/shard-id?metric=cpu.usage
     */
    @GetMapping("/shard-id")
    public ResponseEntity<Map<String, Object>> getShardId(@RequestParam String metric) {
        try {
            int shardId = shardManager.getShardId(metric);
            
            Map<String, Object> response = new HashMap<>();
            response.put("metric", metric);
            response.put("shardId", shardId);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get shard ID for metric: {}", metric, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get the leader node for a metric.
     * GET /api/v1/shard/leader?metric=cpu.usage
     */
    @GetMapping("/leader")
    public ResponseEntity<Map<String, Object>> getLeaderForMetric(@RequestParam String metric) {
        try {
            String leaderNodeId = shardManager.getLeaderForMetric(metric);
            
            Map<String, Object> response = new HashMap<>();
            response.put("metric", metric);
            response.put("leaderNodeId", leaderNodeId);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get leader for metric: {}", metric, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get shard health status.
     * GET /api/v1/shard/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getShardHealth() {
        try {
            Map<Integer, ShardManager.ShardStats> stats = shardManager.getShardStats();
            
            int totalShards = stats.size();
            int leaderShards = (int) stats.values().stream()
                    .filter(ShardManager.ShardStats::isLeader)
                    .count();
            int followerShards = totalShards - leaderShards;
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "healthy");
            response.put("totalShards", totalShards);
            response.put("leaderShards", leaderShards);
            response.put("followerShards", followerShards);
            response.put("shardStats", stats);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get shard health", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "unhealthy");
            response.put("error", e.getMessage());
            
            return ResponseEntity.status(503).body(response);
        }
    }
} 