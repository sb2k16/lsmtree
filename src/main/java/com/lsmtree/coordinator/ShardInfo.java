package com.lsmtree.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ShardInfo {
    private final int shardId;
    private final String leaderNodeId;
    private final List<String> replicaNodeIds;
    private final String status;

    @JsonCreator
    public ShardInfo(@JsonProperty("shardId") int shardId, 
                    @JsonProperty("leaderNodeId") String leaderNodeId, 
                    @JsonProperty("replicaNodeIds") List<String> replicaNodeIds, 
                    @JsonProperty("status") String status) {
        this.shardId = shardId;
        this.leaderNodeId = leaderNodeId;
        this.replicaNodeIds = replicaNodeIds;
        this.status = status;
    }

    public int getShardId() { return shardId; }
    public String getLeaderNodeId() { return leaderNodeId; }
    public List<String> getReplicaNodeIds() { return replicaNodeIds; }
    public String getStatus() { return status; }
} 