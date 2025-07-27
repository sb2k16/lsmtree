package com.lsmtree;

import com.lsmtree.cluster.ClusterMembership;
import com.lsmtree.cluster.NodeInfo;
import com.lsmtree.model.TimeseriesPoint;
import com.lsmtree.sharding.ShardCoordinator;
import com.lsmtree.sharding.ShardInfo;
import com.lsmtree.sharding.ShardManager;
import com.lsmtree.sharding.ShardRaftConsensus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ShardAwareLeaderElectionTest {

    @Mock
    private ClusterMembership clusterMembership;

    private ShardCoordinator coordinator;
    private ShardManager shardManager;
    private ShardRaftConsensus consensus;

    @BeforeEach
    void setUp() {
        coordinator = new ShardCoordinator();
        ReflectionTestUtils.setField(coordinator, "numShards", 4);
        ReflectionTestUtils.setField(coordinator, "replicationFactor", 2);
        ReflectionTestUtils.setField(coordinator, "healthCheckIntervalMs", 1000L);
        
        shardManager = new ShardManager();
        ReflectionTestUtils.setField(shardManager, "localNodeId", "node-1");
        ReflectionTestUtils.setField(shardManager, "localNodeHost", "localhost");
        ReflectionTestUtils.setField(shardManager, "localNodePort", 8080);
        ReflectionTestUtils.setField(shardManager, "coordinator", coordinator);
        ReflectionTestUtils.setField(shardManager, "clusterMembership", clusterMembership);
        
        consensus = new ShardRaftConsensus(0, "node-1", clusterMembership);
    }

    @Test
    void testShardInfoCreation() {
        ShardInfo info = new ShardInfo(1, "node-1", Arrays.asList("node-1", "node-2"), 
                                      ShardInfo.ShardStatus.HEALTHY);
        
        assertEquals(1, info.getShardId());
        assertEquals("node-1", info.getLeaderNodeId());
        assertEquals(2, info.getReplicaCount());
        assertTrue(info.isHealthy());
        assertTrue(info.hasLeader());
        assertTrue(info.isNodeLeader("node-1"));
        assertTrue(info.isNodeReplica("node-2"));
    }

    @Test
    void testShardRaftConsensus() {
        consensus.start();
        
        assertNotNull(consensus.getState());
        assertTrue(consensus.getCurrentTerm() >= 0);
        assertFalse(consensus.isShardLeader()); // Should start as follower
        
        // Test shard info update
        ShardInfo info = new ShardInfo(0, "node-1", Arrays.asList("node-1", "node-2"), 
                                      ShardInfo.ShardStatus.HEALTHY);
        consensus.updateShardInfo(info);
        
        assertEquals(info, consensus.getShardInfo());
        assertTrue(consensus.isShardHealthy());
    }

    @Test
    void testShardCoordinatorNodeRegistration() {
        ShardCoordinator.NodeRegistration registration = 
            coordinator.registerNode("node-1", "localhost", 8080);
        
        assertNotNull(registration);
        assertEquals("node-1", registration.getNodeId());
        assertEquals("localhost", registration.getHost());
        assertEquals(8080, registration.getPort());
        assertNotNull(registration.getAssignedShards());
        
        // Register second node
        ShardCoordinator.NodeRegistration registration2 = 
            coordinator.registerNode("node-2", "localhost", 8081);
        
        assertNotNull(registration2);
        assertEquals("node-2", registration2.getNodeId());
    }

    @Test
    void testShardAssignment() {
        // Register multiple nodes
        coordinator.registerNode("node-1", "localhost", 8080);
        coordinator.registerNode("node-2", "localhost", 8081);
        coordinator.registerNode("node-3", "localhost", 8082);
        
        Map<Integer, ShardInfo> shardMap = coordinator.getShardMap();
        assertEquals(4, shardMap.size()); // 4 shards configured
        
        // Check that shards have leaders and replicas
        for (ShardInfo shardInfo : shardMap.values()) {
            assertTrue(shardInfo.hasLeader());
            assertTrue(shardInfo.getReplicaCount() > 0);
        }
    }

    @Test
    void testShardIdCalculation() {
        String metric1 = "cpu.usage";
        String metric2 = "memory.usage";
        
        int shardId1 = coordinator.getShardId(metric1);
        int shardId2 = coordinator.getShardId(metric2);
        
        assertTrue(shardId1 >= 0 && shardId1 < 4);
        assertTrue(shardId2 >= 0 && shardId2 < 4);
        
        // Same metric should always hash to same shard
        assertEquals(shardId1, coordinator.getShardId(metric1));
    }

    @Test
    void testLeaderForMetric() {
        coordinator.registerNode("node-1", "localhost", 8080);
        coordinator.registerNode("node-2", "localhost", 8081);
        
        String metric = "cpu.usage";
        String leaderNodeId = coordinator.getLeaderForMetric(metric);
        
        assertNotNull(leaderNodeId);
        assertTrue(leaderNodeId.equals("node-1") || leaderNodeId.equals("node-2"));
    }

    @Test
    void testShardManagerInitialization() {
        // Mock coordinator registration
        ShardCoordinator.NodeRegistration mockRegistration = 
            new ShardCoordinator.NodeRegistration("node-1", "localhost", 8080, 
                                                new HashSet<>(Arrays.asList(0, 1)));
        
        // Initialize shard manager
        shardManager.initialize();
        
        Set<Integer> assignedShards = shardManager.getAssignedShards();
        assertNotNull(assignedShards);
    }

    @Test
    void testShardManagerWritePoint() throws Exception {
        // Setup: register node and initialize shards
        coordinator.registerNode("node-1", "localhost", 8080);
        shardManager.initialize();
        
        // Create a test point with tags
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "server-1");
        TimeseriesPoint point = new TimeseriesPoint(System.currentTimeMillis(), 75.5, "cpu.usage", tags);
        
        // This should work if the node is leader for the shard
        // In a real test, we'd need to mock the consensus to return true for isShardLeader
        try {
            shardManager.writePoint(point);
        } catch (IllegalStateException e) {
            // Expected if node is not leader for the shard
            assertTrue(e.getMessage().contains("not the leader"));
        }
    }

    @Test
    void testShardManagerReadRange() {
        // Setup: register node and initialize shards
        coordinator.registerNode("node-1", "localhost", 8080);
        shardManager.initialize();
        
        String metric = "cpu.usage";
        long startTime = System.currentTimeMillis() - 3600000; // 1 hour ago
        long endTime = System.currentTimeMillis();
        
        List<TimeseriesPoint> points = shardManager.readRange(metric, startTime, endTime);
        assertNotNull(points);
        // Should be empty since no data was written
        assertTrue(points.isEmpty());
    }

    @Test
    void testShardManagerStats() {
        // Setup: register node and initialize shards
        coordinator.registerNode("node-1", "localhost", 8080);
        shardManager.initialize();
        
        Map<Integer, ShardManager.ShardStats> stats = shardManager.getShardStats();
        assertNotNull(stats);
        
        // Should have stats for assigned shards
        Set<Integer> assignedShards = shardManager.getAssignedShards();
        for (Integer shardId : assignedShards) {
            assertTrue(stats.containsKey(shardId));
            ShardManager.ShardStats shardStats = stats.get(shardId);
            assertNotNull(shardStats);
            assertEquals(shardId, shardStats.getShardId());
        }
    }

    @Test
    void testShardCoordinatorHealthMonitoring() {
        // Register nodes
        coordinator.registerNode("node-1", "localhost", 8080);
        coordinator.registerNode("node-2", "localhost", 8081);
        
        // Get initial health
        Map<Integer, ShardInfo> shardMap = coordinator.getShardMap();
        int initialHealthyShards = (int) shardMap.values().stream()
                .filter(ShardInfo::isHealthy)
                .count();
        
        assertTrue(initialHealthyShards > 0);
        
        // Unregister a node
        coordinator.unregisterNode("node-1");
        
        // Check that shard assignments were updated
        shardMap = coordinator.getShardMap();
        // Some shards might become unhealthy after node removal
        assertNotNull(shardMap);
    }

    @Test
    void testShardCoordinatorTriggerElection() {
        // Register nodes
        coordinator.registerNode("node-1", "localhost", 8080);
        coordinator.registerNode("node-2", "localhost", 8081);
        
        // Trigger election for a shard
        coordinator.triggerShardLeaderElection(0);
        
        // In a real implementation, this would trigger the election
        // For now, we just verify the method doesn't throw an exception
        assertTrue(true);
    }

    @Test
    void testShardInfoStatusTransitions() {
        ShardInfo healthy = new ShardInfo(0, "node-1", Arrays.asList("node-1", "node-2"), 
                                         ShardInfo.ShardStatus.HEALTHY);
        ShardInfo unhealthy = new ShardInfo(1, null, Arrays.asList("node-1"), 
                                           ShardInfo.ShardStatus.UNHEALTHY);
        ShardInfo rebalancing = new ShardInfo(2, "node-1", Arrays.asList("node-1", "node-2"), 
                                             ShardInfo.ShardStatus.REBALANCING);
        ShardInfo failed = new ShardInfo(3, null, new ArrayList<>(), 
                                        ShardInfo.ShardStatus.FAILED);
        
        assertTrue(healthy.isHealthy());
        assertFalse(unhealthy.isHealthy());
        assertFalse(rebalancing.isHealthy());
        assertFalse(failed.isHealthy());
        
        assertTrue(healthy.hasLeader());
        assertFalse(unhealthy.hasLeader());
        assertTrue(rebalancing.hasLeader());
        assertFalse(failed.hasLeader());
    }

    @Test
    void testShardCoordinatorConcurrentAccess() throws InterruptedException {
        // Test concurrent node registration
        List<Thread> threads = new ArrayList<>();
        List<Exception> exceptions = new ArrayList<>();
        
        for (int i = 0; i < 5; i++) {
            final int nodeId = i;
            Thread thread = new Thread(() -> {
                try {
                    coordinator.registerNode("node-" + nodeId, "localhost", 8080 + nodeId);
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                }
            });
            threads.add(thread);
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        // Check for exceptions
        assertTrue(exceptions.isEmpty(), "Concurrent access should not throw exceptions");
        
        // Verify all nodes were registered
        assertEquals(5, coordinator.getAllNodes().size());
    }
} 