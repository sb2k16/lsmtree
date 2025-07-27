package com.lsmtree.api;

import com.lsmtree.sharding.ShardCoordinator;
import com.lsmtree.sharding.ShardInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/coordinator")
public class CoordinatorController {
    
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorController.class);
    
    @Autowired
    private ShardCoordinator coordinator;

    /**
     * Get the current shard map.
     * GET /api/v1/coordinator/shard-map
     */
    @GetMapping("/shard-map")
    public ResponseEntity<Map<Integer, ShardInfo>> getShardMap() {
        try {
            Map<Integer, ShardInfo> shardMap = coordinator.getShardMap();
            return ResponseEntity.ok(shardMap);
        } catch (Exception e) {
            logger.error("Failed to get shard map", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get the leader for a specific metric.
     * GET /api/v1/coordinator/leader?metric=cpu.usage
     */
    @GetMapping("/leader")
    public ResponseEntity<Map<String, Object>> getLeaderForMetric(@RequestParam String metric) {
        try {
            String leaderNodeId = coordinator.getLeaderForMetric(metric);
            int shardId = coordinator.getShardId(metric);
            
            Map<String, Object> response = new HashMap<>();
            response.put("metric", metric);
            response.put("shardId", shardId);
            response.put("leaderNodeId", leaderNodeId);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get leader for metric: {}", metric, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get the shard ID for a metric.
     * GET /api/v1/coordinator/shard-id?metric=cpu.usage
     */
    @GetMapping("/shard-id")
    public ResponseEntity<Map<String, Object>> getShardId(@RequestParam String metric) {
        try {
            int shardId = coordinator.getShardId(metric);
            
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
     * Register a node with the coordinator.
     * POST /api/v1/coordinator/nodes
     */
    @PostMapping("/nodes")
    public ResponseEntity<ShardCoordinator.NodeRegistration> registerNode(@RequestBody NodeRegistrationRequest request) {
        try {
            ShardCoordinator.NodeRegistration registration = 
                coordinator.registerNode(request.getNodeId(), request.getHost(), request.getPort());
            
            logger.info("Registered node: {}", registration);
            return ResponseEntity.ok(registration);
        } catch (Exception e) {
            logger.error("Failed to register node", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Unregister a node from the coordinator.
     * DELETE /api/v1/coordinator/nodes/{nodeId}
     */
    @DeleteMapping("/nodes/{nodeId}")
    public ResponseEntity<Map<String, Object>> unregisterNode(@PathVariable String nodeId) {
        try {
            coordinator.unregisterNode(nodeId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Node unregistered successfully");
            response.put("nodeId", nodeId);
            
            logger.info("Unregistered node: {}", nodeId);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to unregister node: {}", nodeId, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Trigger leader election for a specific shard.
     * POST /api/v1/coordinator/shard/{shardId}/election
     */
    @PostMapping("/shard/{shardId}/election")
    public ResponseEntity<Map<String, Object>> triggerShardElection(@PathVariable int shardId) {
        try {
            coordinator.triggerShardLeaderElection(shardId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Leader election triggered for shard");
            response.put("shardId", shardId);
            
            logger.info("Triggered leader election for shard: {}", shardId);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to trigger election for shard: {}", shardId, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get coordinator health status.
     * GET /api/v1/coordinator/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        try {
            Map<Integer, ShardInfo> shardMap = coordinator.getShardMap();
            
            int totalShards = shardMap.size();
            int healthyShards = (int) shardMap.values().stream()
                    .filter(ShardInfo::isHealthy)
                    .count();
            int unhealthyShards = totalShards - healthyShards;
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "healthy");
            response.put("totalShards", totalShards);
            response.put("healthyShards", healthyShards);
            response.put("unhealthyShards", unhealthyShards);
            response.put("healthPercentage", totalShards > 0 ? (double) healthyShards / totalShards * 100 : 0);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get coordinator health", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "unhealthy");
            response.put("error", e.getMessage());
            
            return ResponseEntity.status(503).body(response);
        }
    }

    /**
     * Get all registered nodes.
     * GET /api/v1/coordinator/nodes
     */
    @GetMapping("/nodes")
    public ResponseEntity<Map<String, Object>> getAllNodes() {
        try {
            var nodes = coordinator.getAllNodes();
            
            Map<String, Object> response = new HashMap<>();
            response.put("nodes", nodes);
            response.put("totalNodes", nodes.size());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get all nodes", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Node registration request.
     */
    public static class NodeRegistrationRequest {
        private String nodeId;
        private String host;
        private int port;

        public NodeRegistrationRequest() {}

        public NodeRegistrationRequest(String nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }

        // Getters and Setters
        public String getNodeId() { return nodeId; }
        public void setNodeId(String nodeId) { this.nodeId = nodeId; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
    }
} 