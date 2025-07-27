package com.lsmtree.sharding;

import com.lsmtree.cluster.ClusterMembership;
import com.lsmtree.cluster.RaftConsensus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Shard-aware Raft consensus that manages leader election for a specific shard.
 * Each shard has its own Raft consensus instance.
 */
public class ShardRaftConsensus {
    
    private static final Logger logger = LoggerFactory.getLogger(ShardRaftConsensus.class);
    
    private final int shardId;
    private final String nodeId;
    private final RaftConsensus raftConsensus;
    private final AtomicReference<ShardInfo> shardInfo;

    public ShardRaftConsensus(int shardId, String nodeId, ClusterMembership membership) {
        this.shardId = shardId;
        this.nodeId = nodeId;
        this.raftConsensus = new RaftConsensus(nodeId, membership);
        this.shardInfo = new AtomicReference<>();
    }

    /**
     * Starts the Raft consensus for this shard.
     */
    public void start() {
        logger.info("Starting Raft consensus for shard {}", shardId);
        raftConsensus.start();
    }

    /**
     * Starts an election for this shard.
     */
    public void startElection() {
        logger.info("Starting election for shard {}", shardId);
        raftConsensus.startElection();
    }

    /**
     * Checks if this node is the leader for this shard.
     */
    public boolean isShardLeader() {
        return raftConsensus.isLeader();
    }

    /**
     * Gets the current Raft state for this shard.
     */
    public RaftConsensus.RaftState getState() {
        return raftConsensus.getState();
    }

    /**
     * Gets the current term for this shard.
     */
    public long getCurrentTerm() {
        return raftConsensus.getCurrentTerm();
    }

    /**
     * Gets the ID of the node we voted for in this shard.
     */
    public String getVotedFor() {
        return raftConsensus.getVotedFor();
    }

    /**
     * Gets the last heartbeat time for this shard.
     */
    public long getLastHeartbeat() {
        return raftConsensus.getLastHeartbeat();
    }

    /**
     * Handles a vote request for this shard.
     */
    public RaftConsensus.VoteResponse handleVoteRequest(RaftConsensus.VoteRequest request) {
        return raftConsensus.handleVoteRequest(request);
    }

    /**
     * Handles a heartbeat for this shard.
     */
    public RaftConsensus.HeartbeatResponse handleHeartbeat(RaftConsensus.HeartbeatRequest request) {
        return raftConsensus.handleHeartbeat(request);
    }

    /**
     * Checks if election timeout has occurred for this shard.
     */
    public void checkElectionTimeout() {
        raftConsensus.checkElectionTimeout();
    }

    /**
     * Gets the shard ID this consensus manages.
     */
    public int getShardId() {
        return shardId;
    }

    /**
     * Gets the node ID running this consensus.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Updates the shard information.
     */
    public void updateShardInfo(ShardInfo info) {
        if (info.getShardId() != shardId) {
            logger.warn("Attempted to update shard info with wrong shard ID: expected {}, got {}", 
                       shardId, info.getShardId());
            return;
        }
        
        ShardInfo oldInfo = shardInfo.getAndSet(info);
        if (oldInfo == null || !oldInfo.equals(info)) {
            logger.info("Updated shard info for shard {}: leader={}, status={}", 
                       shardId, info.getLeaderNodeId(), info.getStatus());
        }
    }

    /**
     * Gets the current shard information.
     */
    public ShardInfo getShardInfo() {
        return shardInfo.get();
    }

    /**
     * Checks if this shard is healthy.
     */
    public boolean isShardHealthy() {
        ShardInfo info = shardInfo.get();
        return info != null && info.isHealthy();
    }

    /**
     * Gets the underlying Raft consensus instance.
     */
    public RaftConsensus getRaftConsensus() {
        return raftConsensus;
    }

    @Override
    public String toString() {
        return String.format("ShardRaftConsensus{shardId=%d, nodeId='%s', state=%s, isLeader=%s}",
                shardId, nodeId, getState(), isShardLeader());
    }
} 