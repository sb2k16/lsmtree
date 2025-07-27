package com.lsmtree.api;

import com.lsmtree.cluster.ClusterManager;
import com.lsmtree.cluster.NodeInfo;
import com.lsmtree.replication.ReplicationManager;
import com.lsmtree.storage.ReplicatedLSMTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API controller for cluster management operations.
 */
@RestController
@RequestMapping("/api/v1/cluster")
public class ClusterController {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterController.class);
    
    @Autowired
    private ClusterManager clusterManager;
    
    @Autowired
    private ReplicatedLSMTree replicatedLSMTree;
    
    @Autowired
    private ReplicationManager replicationManager;

    /**
     * Get cluster status.
     * GET /api/v1/cluster/status
     */
    @GetMapping("/status")
    public ResponseEntity<ClusterManager.ClusterStatus> getClusterStatus() {
        try {
            return ResponseEntity.ok(clusterManager.getClusterStatus());
        } catch (Exception e) {
            logger.error("Failed to get cluster status", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get all nodes in the cluster.
     * GET /api/v1/cluster/nodes
     */
    @GetMapping("/nodes")
    public ResponseEntity<List<NodeInfo>> getNodes() {
        try {
            return ResponseEntity.ok(clusterManager.getAllNodes());
        } catch (Exception e) {
            logger.error("Failed to get cluster nodes", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Add a node to the cluster.
     * POST /api/v1/cluster/nodes
     */
    @PostMapping("/nodes")
    public ResponseEntity<Map<String, Object>> addNode(@RequestBody AddNodeRequest request) {
        try {
            if (request.getNodeId() == null || request.getHost() == null) {
                return ResponseEntity.badRequest().body(createErrorResponse("Invalid request", "nodeId and host are required"));
            }
            
            clusterManager.addNode(request.getNodeId(), request.getHost(), request.getPort());
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Node added successfully");
            response.put("nodeId", request.getNodeId());
            response.put("host", request.getHost());
            response.put("port", request.getPort());
            
            logger.info("Added node to cluster: {}", request.getNodeId());
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to add node", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to add node", e.getMessage()));
        }
    }

    /**
     * Remove a node from the cluster.
     * DELETE /api/v1/cluster/nodes/{nodeId}
     */
    @DeleteMapping("/nodes/{nodeId}")
    public ResponseEntity<Map<String, Object>> removeNode(@PathVariable String nodeId) {
        try {
            clusterManager.removeNode(nodeId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Node removed successfully");
            response.put("nodeId", nodeId);
            
            logger.info("Removed node from cluster: {}", nodeId);
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to remove node", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to remove node", e.getMessage()));
        }
    }

    /**
     * Force a leader election.
     * POST /api/v1/cluster/leader/election
     */
    @PostMapping("/leader/election")
    public ResponseEntity<Map<String, Object>> forceElection() {
        try {
            clusterManager.forceElection();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Leader election initiated");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to force election", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get cluster statistics.
     * GET /api/v1/cluster/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<ReplicatedLSMTree.ClusterStats> getClusterStats() {
        try {
            ReplicatedLSMTree.ClusterStats stats = replicatedLSMTree.getClusterStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Failed to get cluster stats", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Get replication statistics.
     * GET /api/v1/cluster/replication/stats
     */
    @GetMapping("/replication/stats")
    public ResponseEntity<ReplicationManager.ReplicationStats> getReplicationStats() {
        try {
            ReplicationManager.ReplicationStats stats = replicationManager.getStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Failed to get replication stats", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Get replication lag information.
     * GET /api/v1/cluster/replication/lag
     */
    @GetMapping("/replication/lag")
    public ResponseEntity<Map<String, Object>> getReplicationLag() {
        try {
            ReplicationManager.ReplicationStats stats = replicationManager.getStats();
            
            Map<String, Object> response = new HashMap<>();
            response.put("totalReplications", stats.getTotalReplications());
            response.put("successfulReplications", stats.getSuccessfulReplications());
            response.put("failedReplications", stats.getFailedReplications());
            response.put("successRate", stats.getSuccessRate());
            response.put("targetStats", stats.getTargetStats());
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to get replication lag", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Force data synchronization.
     * POST /api/v1/cluster/replication/sync
     */
    @PostMapping("/replication/sync")
    public ResponseEntity<Map<String, Object>> forceSync(@RequestBody SyncRequest request) {
        try {
            if (!clusterManager.isLeader()) {
                return ResponseEntity.status(HttpStatus.FORBIDDEN)
                        .body(createErrorResponse("Not leader", "Only leader can initiate sync"));
            }
            
            // Update replication targets
            replicatedLSMTree.updateReplicationTargets();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Data synchronization initiated");
            response.put("targetNode", request.getTargetNode());
            response.put("fromTimestamp", request.getFromTimestamp());
            response.put("timestamp", System.currentTimeMillis());
            
            logger.info("Forced data sync for node: {}", request.getTargetNode());
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to force sync", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to force sync", e.getMessage()));
        }
    }

    /**
     * Get cluster health information.
     * GET /api/v1/cluster/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getClusterHealth() {
        try {
            ClusterManager.ClusterStatus status = clusterManager.getClusterStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", status.hasQuorum() ? "healthy" : "unhealthy");
            response.put("hasQuorum", status.hasQuorum());
            response.put("isLeader", status.isLeader());
            response.put("raftState", status.getRaftState());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get cluster health", e);
            return ResponseEntity.internalServerError().build();
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
        response.put("timestamp", System.currentTimeMillis());
        return response;
    }

    /**
     * Request for adding a node to the cluster.
     */
    public static class AddNodeRequest {
        private String nodeId;
        private String host;
        private int port;

        public AddNodeRequest() {}

        public AddNodeRequest(String nodeId, String host, int port) {
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

    /**
     * Request for data synchronization.
     */
    public static class SyncRequest {
        private String targetNode;
        private long fromTimestamp;

        public SyncRequest() {}

        public SyncRequest(String targetNode, long fromTimestamp) {
            this.targetNode = targetNode;
            this.fromTimestamp = fromTimestamp;
        }

        // Getters and Setters
        public String getTargetNode() { return targetNode; }
        public void setTargetNode(String targetNode) { this.targetNode = targetNode; }
        public long getFromTimestamp() { return fromTimestamp; }
        public void setFromTimestamp(long fromTimestamp) { this.fromTimestamp = fromTimestamp; }
    }
} 