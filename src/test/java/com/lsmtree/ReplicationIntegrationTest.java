package com.lsmtree;

import com.lsmtree.cluster.ClusterManager;
import com.lsmtree.cluster.ClusterMembership;
import com.lsmtree.cluster.RaftConsensus;
import com.lsmtree.model.QueryRequest;
import com.lsmtree.model.QueryResponse;
import com.lsmtree.model.TimeseriesPoint;
import com.lsmtree.replication.ReplicationConfig;
import com.lsmtree.replication.ReplicationManager;
import com.lsmtree.service.TimeseriesService;
import com.lsmtree.storage.ReplicatedLSMTree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class ReplicationIntegrationTest {

    @Mock
    private ReplicatedLSMTree replicatedLSMTree;
    @Mock
    private TimeseriesService timeseriesService;
    @Mock
    private ClusterManager clusterManager;
    @Mock
    private ClusterMembership clusterMembership;
    @Mock
    private RaftConsensus raftConsensus;
    @Mock
    private ReplicationManager replicationManager;
    @Mock
    private ReplicationConfig replicationConfig;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // Manually inject mocks if needed
        // e.g., use reflection or setters if available
    }

    @Test
    void testClusterInitialization() {
        // Test Phase 1: Cluster Foundation
        assertNotNull(clusterManager);
        assertNotNull(clusterMembership);
        assertNotNull(raftConsensus);

        // Verify local node is added
        var localNode = clusterManager.getLocalNode();
        assertNotNull(localNode);
        assertEquals("test-node-1", localNode.getNodeId());
        assertEquals("localhost", localNode.getHost());
        assertEquals(8080, localNode.getPort());
    }

    @Test
    void testRaftConsensus() {
        // Test Phase 1: Raft Consensus
        assertNotNull(raftConsensus);
        
        // Verify initial state
        assertEquals(RaftConsensus.RaftState.FOLLOWER, raftConsensus.getState());
        assertEquals(0, raftConsensus.getCurrentTerm());
        
        // Test leader election
        raftConsensus.startElection();
        
        // Wait for election to complete
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Should become leader in single-node cluster
        assertTrue(raftConsensus.isLeader() || clusterManager.isLeader());
    }

    @Test
    void testReplicationConfiguration() {
        // Test Phase 2: Replication Configuration
        assertNotNull(replicationConfig);
        assertEquals(ReplicationConfig.ConsistencyLevel.QUORUM, replicationConfig.getWriteConsistency());
        assertEquals(ReplicationConfig.ConsistencyLevel.ONE, replicationConfig.getReadConsistency());
        assertFalse(replicationConfig.isAsyncReplication());
        assertEquals(5000, replicationConfig.getReplicationTimeoutMs());
    }

    @Test
    void testReplicationManager() {
        // Test Phase 2: Replication Manager
        assertNotNull(replicationManager);
        
        // Test replication targets update
        replicationManager.updateReplicationTargets();
        
        // In single-node test, no replication targets should exist
        var stats = replicationManager.getStats();
        assertNotNull(stats);
        assertEquals(0, stats.getTotalReplications());
    }

    @Test
    void testReplicatedLSMTree() {
        // Test Phase 3: Replicated LSM Tree
        assertNotNull(replicatedLSMTree);
        
        // Verify it extends base functionality
        assertTrue(replicatedLSMTree instanceof ReplicatedLSMTree);
        
        // Test cluster stats
        var clusterStats = replicatedLSMTree.getClusterStats();
        assertNotNull(clusterStats);
        assertEquals(1, clusterStats.getTotalNodes()); // Single node test
        assertTrue(clusterStats.isLeader()); // Should be leader in single-node
    }

    @Test
    void testWriteReplication() throws IOException {
        // Test Phase 3: Write Replication
        TimeseriesPoint point = new TimeseriesPoint(System.currentTimeMillis(), 100.0, "test.metric");
        
        // Write should succeed even in single-node cluster
        timeseriesService.writePoint(point);
        
        // Verify point was written
        QueryRequest request = new QueryRequest("test.metric", 
            System.currentTimeMillis() - 1000, System.currentTimeMillis() + 1000);
        QueryResponse response = timeseriesService.query(request);
        
        assertEquals(1, response.getTotalPoints());
    }

    @Test
    void testBatchWriteReplication() throws IOException {
        // Test Phase 3: Batch Write Replication
        List<TimeseriesPoint> points = new ArrayList<>();
        long baseTime = System.currentTimeMillis();
        
        for (int i = 0; i < 10; i++) {
            points.add(new TimeseriesPoint(baseTime + i * 1000, 100.0 + i, "batch.metric"));
        }
        
        // Batch write should succeed
        timeseriesService.writeBatch(points);
        
        // Verify points were written
        QueryRequest request = new QueryRequest("batch.metric", 
            baseTime - 1000, baseTime + 11000);
        QueryResponse response = timeseriesService.query(request);
        
        assertEquals(10, response.getTotalPoints());
    }

    @Test
    void testClusterHealthMonitoring() {
        // Test Phase 4: Health Monitoring
        var clusterStatus = clusterManager.getClusterStatus();
        assertNotNull(clusterStatus);
        
        // In single-node test, should have quorum
        assertTrue(clusterStatus.hasQuorum());
        
        // Should be leader
        assertTrue(clusterStatus.isLeader());
        
        // Raft state should be LEADER
        assertEquals(RaftConsensus.RaftState.LEADER, clusterStatus.getRaftState());
    }

    @Test
    void testClusterStatistics() {
        // Test Phase 4: Statistics and Monitoring
        var clusterStats = replicatedLSMTree.getClusterStats();
        assertNotNull(clusterStats);
        
        // Verify basic stats
        assertEquals(1, clusterStats.getTotalNodes());
        assertEquals(1, clusterStats.getOnlineNodes());
        assertTrue(clusterStats.isLeader());
        
        // Test replication stats
        var replicationStats = replicatedLSMTree.getReplicationStats();
        assertNotNull(replicationStats);
        assertEquals(0, replicationStats.getTotalReplications()); // No followers in test
    }

    @Test
    void testServiceLayerIntegration() {
        // Test complete service layer integration
        assertNotNull(timeseriesService);
        
        // Test cluster status
        var status = timeseriesService.getClusterStatus();
        assertNotNull(status);
        assertTrue(status.hasQuorum());
        
        // Test leader check
        assertTrue(timeseriesService.isLeader());
    }

    @Test
    void testFailoverCapability() {
        // Test Phase 4: Failover (simulated)
        // Force election to test failover mechanism
        clusterManager.forceElection();
        
        // Wait for election
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Should still be leader in single-node
        assertTrue(clusterManager.isLeader());
    }

    @Test
    void testConfigurationProperties() {
        // Test configuration loading
        assertEquals("QUORUM", replicationConfig.getWriteConsistency().name());
        assertEquals("ONE", replicationConfig.getReadConsistency().name());
        assertFalse(replicationConfig.isAsyncReplication());
        assertTrue(replicationConfig.isAutoFailover());
        assertEquals(5000, replicationConfig.getReplicationTimeoutMs());
        assertEquals(3, replicationConfig.getMaxRetries());
    }

    @Test
    void testConcurrentOperations() throws InterruptedException {
        // Test concurrent write operations
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < 5; i++) {
            final int index = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    TimeseriesPoint concurrentPoint = new TimeseriesPoint(System.currentTimeMillis(), 100.0, "concurrent.metric." + index);
                    timeseriesService.writePoint(concurrentPoint);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }
        
        // Wait for all operations to complete
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(10, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        
        // Verify all points were written
        QueryRequest request = new QueryRequest("concurrent.metric", 
            0, System.currentTimeMillis() + 1000);
        QueryResponse response = timeseriesService.query(request);
        
        assertEquals(5, response.getTotalPoints());
    }
} 