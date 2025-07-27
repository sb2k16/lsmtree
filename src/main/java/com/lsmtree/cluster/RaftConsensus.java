package com.lsmtree.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implements the Raft consensus protocol for leader election.
 * Manages the state machine for leader election and term management.
 */
public class RaftConsensus {
    
    private static final Logger logger = LoggerFactory.getLogger(RaftConsensus.class);
    
    public enum RaftState {
        FOLLOWER, CANDIDATE, LEADER
    }
    
    private final String nodeId;
    private final ClusterMembership membership;
    private final AtomicReference<RaftState> state;
    private final AtomicLong currentTerm;
    private final AtomicReference<String> votedFor;
    private final AtomicLong lastHeartbeat;
    private final AtomicLong electionTimeout;
    private final AtomicLong heartbeatInterval;
    
    private final ReentrantLock stateLock;
    private volatile long lastElectionTime;
    private volatile boolean electionInProgress;

    public RaftConsensus(String nodeId, ClusterMembership membership) {
        this.nodeId = nodeId;
        this.membership = membership;
        this.state = new AtomicReference<>(RaftState.FOLLOWER);
        this.currentTerm = new AtomicLong(0);
        this.votedFor = new AtomicReference<>(null);
        this.lastHeartbeat = new AtomicLong(System.currentTimeMillis());
        this.electionTimeout = new AtomicLong(1000 + (long) (Math.random() * 1000)); // 1-2 seconds
        this.heartbeatInterval = new AtomicLong(100); // 100ms
        this.stateLock = new ReentrantLock();
        this.lastElectionTime = 0;
        this.electionInProgress = false;
    }

    /**
     * Starts the consensus process.
     */
    public void start() {
        logger.info("Starting Raft consensus for node: {}", nodeId);
        scheduleElectionTimeout();
    }

    /**
     * Handles a vote request from a candidate.
     */
    public VoteResponse handleVoteRequest(VoteRequest request) {
        stateLock.lock();
        try {
            logger.debug("Received vote request from {} for term {}", 
                        request.getCandidateId(), request.getTerm());
            
            // If request term is less than current term, reject
            if (request.getTerm() < currentTerm.get()) {
                return new VoteResponse(currentTerm.get(), false);
            }
            
            // If request term is greater than current term, become follower
            if (request.getTerm() > currentTerm.get()) {
                becomeFollower(request.getTerm());
            }
            
            // Check if we can vote for this candidate
            boolean canVote = (votedFor.get() == null || votedFor.get().equals(request.getCandidateId())) &&
                             request.getLastLogTerm() >= getLastLogTerm() &&
                             request.getLastLogIndex() >= getLastLogIndex();
            
            if (canVote) {
                votedFor.set(request.getCandidateId());
                lastHeartbeat.set(System.currentTimeMillis());
                logger.info("Voted for {} in term {}", request.getCandidateId(), request.getTerm());
                return new VoteResponse(currentTerm.get(), true);
            } else {
                logger.debug("Rejected vote for {} in term {}", request.getCandidateId(), request.getTerm());
                return new VoteResponse(currentTerm.get(), false);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Handles a heartbeat from the leader.
     */
    public HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        stateLock.lock();
        try {
            logger.debug("Received heartbeat from {} for term {}", 
                        request.getLeaderId(), request.getTerm());
            
            // If request term is less than current term, reject
            if (request.getTerm() < currentTerm.get()) {
                return new HeartbeatResponse(currentTerm.get(), false);
            }
            
            // If request term is greater than current term, become follower
            if (request.getTerm() > currentTerm.get()) {
                becomeFollower(request.getTerm());
            }
            
            // Update heartbeat and become follower if we were candidate
            lastHeartbeat.set(System.currentTimeMillis());
            if (state.get() == RaftState.CANDIDATE) {
                becomeFollower(currentTerm.get());
            }
            
            // Update leader in membership
            NodeInfo leader = membership.getNode(request.getLeaderId());
            if (leader != null) {
                leader.setRole(NodeInfo.NodeRole.LEADER);
                leader.setStatus(NodeInfo.NodeStatus.ONLINE);
                leader.setLastHeartbeat(System.currentTimeMillis());
                membership.updateNode(leader);
            }
            
            return new HeartbeatResponse(currentTerm.get(), true);
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Starts an election for leadership.
     */
    public void startElection() {
        stateLock.lock();
        try {
            if (electionInProgress) {
                logger.debug("Election already in progress, skipping");
                return;
            }
            
            logger.info("Starting election for term {}", currentTerm.get() + 1);
            electionInProgress = true;
            lastElectionTime = System.currentTimeMillis();
            
            // Increment term and become candidate
            long newTerm = currentTerm.incrementAndGet();
            becomeCandidate(newTerm);
            
            // Reset vote for self
            votedFor.set(nodeId);
            
            // Request votes from all other nodes
            requestVotesFromPeers(newTerm);
            
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Checks if election timeout has occurred.
     */
    public void checkElectionTimeout() {
        long now = System.currentTimeMillis();
        long timeout = electionTimeout.get();
        
        if (state.get() == RaftState.FOLLOWER && 
            (now - lastHeartbeat.get()) > timeout) {
            logger.info("Election timeout occurred, starting election");
            startElection();
        }
    }

    /**
     * Becomes a follower.
     */
    private void becomeFollower(long term) {
        currentTerm.set(term);
        state.set(RaftState.FOLLOWER);
        votedFor.set(null);
        electionInProgress = false;
        
        // Update local node status
        NodeInfo localNode = membership.getLocalNode();
        if (localNode != null) {
            localNode.setRole(NodeInfo.NodeRole.FOLLOWER);
            localNode.setStatus(NodeInfo.NodeStatus.ONLINE);
            membership.updateNode(localNode);
        }
        
        logger.info("Became follower for term {}", term);
        scheduleElectionTimeout();
    }

    /**
     * Becomes a candidate.
     */
    private void becomeCandidate(long term) {
        state.set(RaftState.CANDIDATE);
        
        // Update local node status
        NodeInfo localNode = membership.getLocalNode();
        if (localNode != null) {
            localNode.setRole(NodeInfo.NodeRole.CANDIDATE);
            localNode.setStatus(NodeInfo.NodeStatus.ONLINE);
            membership.updateNode(localNode);
        }
        
        logger.info("Became candidate for term {}", term);
    }

    /**
     * Becomes the leader.
     */
    public void becomeLeader() {
        stateLock.lock();
        try {
            state.set(RaftState.LEADER);
            electionInProgress = false;
            
            // Update local node status
            NodeInfo localNode = membership.getLocalNode();
            if (localNode != null) {
                localNode.setRole(NodeInfo.NodeRole.LEADER);
                localNode.setStatus(NodeInfo.NodeStatus.ONLINE);
                localNode.setLastHeartbeat(System.currentTimeMillis());
                membership.updateNode(localNode);
                membership.setLeader(localNode);
            }
            
            logger.info("Became leader for term {}", currentTerm.get());
            startHeartbeat();
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Requests votes from all peer nodes.
     */
    private void requestVotesFromPeers(long term) {
        // This would be implemented to send vote requests to all other nodes
        // For now, we'll simulate the voting process
        logger.debug("Requesting votes from peers for term {}", term);
        
        // Simulate receiving votes (in real implementation, this would be async)
        int totalNodes = membership.getAllNodes().size();
        int votesReceived = 1; // Vote for self
        
        if (votesReceived >= membership.getQuorumSize()) {
            becomeLeader();
        } else {
            // Reset election timeout for next attempt
            scheduleElectionTimeout();
        }
    }

    /**
     * Starts sending heartbeats to followers.
     */
    private void startHeartbeat() {
        // This would be implemented to send periodic heartbeats
        logger.debug("Starting heartbeat to followers");
    }

    /**
     * Schedules the next election timeout.
     */
    private void scheduleElectionTimeout() {
        // Reset election timeout with random jitter
        long timeout = 1000 + (long) (Math.random() * 1000);
        electionTimeout.set(timeout);
        logger.debug("Scheduled election timeout in {}ms", timeout);
    }

    /**
     * Gets the current Raft state.
     */
    public RaftState getState() {
        return state.get();
    }

    /**
     * Gets the current term.
     */
    public long getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * Gets the ID of the node we voted for.
     */
    public String getVotedFor() {
        return votedFor.get();
    }

    /**
     * Gets the last heartbeat time.
     */
    public long getLastHeartbeat() {
        return lastHeartbeat.get();
    }

    /**
     * Checks if this node is the leader.
     */
    public boolean isLeader() {
        return state.get() == RaftState.LEADER;
    }

    /**
     * Gets the last log term (placeholder implementation).
     */
    private long getLastLogTerm() {
        return 0; // Would be implemented based on actual log
    }

    /**
     * Gets the last log index (placeholder implementation).
     */
    private long getLastLogIndex() {
        return 0; // Would be implemented based on actual log
    }

    // Request/Response classes for Raft protocol
    public static class VoteRequest {
        private final long term;
        private final String candidateId;
        private final long lastLogIndex;
        private final long lastLogTerm;

        public VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }

        // Getters
        public long getTerm() { return term; }
        public String getCandidateId() { return candidateId; }
        public long getLastLogIndex() { return lastLogIndex; }
        public long getLastLogTerm() { return lastLogTerm; }
    }

    public static class VoteResponse {
        private final long term;
        private final boolean voteGranted;

        public VoteResponse(long term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }

        // Getters
        public long getTerm() { return term; }
        public boolean isVoteGranted() { return voteGranted; }
    }

    public static class HeartbeatRequest {
        private final long term;
        private final String leaderId;
        private final long prevLogIndex;
        private final long prevLogTerm;

        public HeartbeatRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
        }

        // Getters
        public long getTerm() { return term; }
        public String getLeaderId() { return leaderId; }
        public long getPrevLogIndex() { return prevLogIndex; }
        public long getPrevLogTerm() { return prevLogTerm; }
    }

    public static class HeartbeatResponse {
        private final long term;
        private final boolean success;

        public HeartbeatResponse(long term, boolean success) {
            this.term = term;
            this.success = success;
        }

        // Getters
        public long getTerm() { return term; }
        public boolean isSuccess() { return success; }
    }
} 