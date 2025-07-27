package com.lsmtree.coordinator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/api/v1")
public class ProxyController {

    @Autowired
    private ShardRouter shardRouter;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private NodeRegistry nodeRegistry;

    @PostMapping("/ingest")
    public ResponseEntity<?> ingest(@RequestBody TimeseriesPoint point) {
        int shardId = shardRouter.getShardId(point.getMetric());
        NodeInfo leader = shardRouter.getLeaderNode(shardId);
        String url = "http://" + leader.getHost() + ":" + leader.getPort() + "/api/v1/timeseries/write";
        // Forward the request
        ResponseEntity<?> response = restTemplate.postForEntity(url, point, Object.class);
        return response;
    }

    @PostMapping("/query")
    public ResponseEntity<?> query(@RequestBody QueryRequest request) {
        int shardId = shardRouter.getShardId(request.getMetric());
        NodeInfo node = shardRouter.getHealthyNodeForShard(shardId);
        String url = "http://" + node.getHost() + ":" + node.getPort() + "/api/v1/timeseries/query";
        ResponseEntity<?> response = restTemplate.postForEntity(url, request, Object.class);
        return response;
    }

    @PostMapping("/register-node")
    public ResponseEntity<?> registerNode(@RequestBody NodeInfo nodeInfo) {
        nodeRegistry.registerNode(nodeInfo);
        return ResponseEntity.ok("Node registered: " + nodeInfo.getNodeId());
    }

    @PostMapping("/unregister-node")
    public ResponseEntity<?> unregisterNode(@RequestParam String nodeId) {
        nodeRegistry.unregisterNode(nodeId);
        return ResponseEntity.ok("Node unregistered: " + nodeId);
    }

    @PostMapping("/update-shard-map")
    public ResponseEntity<?> updateShardMap(@RequestBody ShardInfo shardInfo) {
        nodeRegistry.updateShardInfo(shardInfo.getShardId(), shardInfo);
        return ResponseEntity.ok("Shard info updated for shard: " + shardInfo.getShardId());
    }

    @GetMapping("/shard-map")
    public ResponseEntity<?> getShardMap() {
        return ResponseEntity.ok(nodeRegistry.getShardMap());
    }

    @GetMapping("/node-health")
    public ResponseEntity<?> getNodeHealth() {
        return ResponseEntity.ok(nodeRegistry.getNodeHealthMap());
    }
} 