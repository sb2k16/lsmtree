package com.lsmtree;

import com.lsmtree.model.QueryRequest;
import com.lsmtree.model.QueryResponse;
import com.lsmtree.model.TimeseriesPoint;
import com.lsmtree.service.TimeseriesService;
import com.lsmtree.storage.ReplicatedLSMTree;
import com.lsmtree.cluster.ClusterManager;
import com.lsmtree.sharding.ShardManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TimeseriesServiceTest {

    private TimeseriesService timeseriesService;
    @Mock
    private ReplicatedLSMTree lsmTree;
    @Mock
    private ClusterManager clusterManager;
    @Mock
    private ShardManager shardManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        timeseriesService = new TimeseriesService();
        try {
            java.lang.reflect.Field f1 = TimeseriesService.class.getDeclaredField("lsmTree");
            f1.setAccessible(true);
            f1.set(timeseriesService, lsmTree);
            java.lang.reflect.Field f2 = TimeseriesService.class.getDeclaredField("clusterManager");
            f2.setAccessible(true);
            f2.set(timeseriesService, clusterManager);
            java.lang.reflect.Field f3 = TimeseriesService.class.getDeclaredField("shardManager");
            f3.setAccessible(true);
            f3.set(timeseriesService, shardManager);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testWriteAndQuerySinglePoint() throws IOException {
        // Create a test point
        TimeseriesPoint point = new TimeseriesPoint(
            1640995200000L,  // timestamp
            85.5,            // value
            "cpu.usage",     // metric
            Map.of("host", "server1", "region", "us-east") // tags
        );

        // Write the point
        timeseriesService.writePoint(point);

        // Query for the point
        QueryRequest request = new QueryRequest(
            "cpu.usage",
            1640995200000L,
            1640995260000L,
            Map.of("host", "server1"),
            1000
        );

        QueryResponse response = timeseriesService.query(request);

        // Verify results
        assertNotNull(response);
        assertEquals("cpu.usage", response.getMetric());
        assertEquals(1, response.getTotalPoints());
        assertFalse(response.isTruncated());
        assertEquals(1, response.getPoints().size());

        TimeseriesPoint retrievedPoint = response.getPoints().get(0);
        assertEquals(point.getTimestamp(), retrievedPoint.getTimestamp());
        assertEquals(point.getValue(), retrievedPoint.getValue(), 0.001);
        assertEquals(point.getMetric(), retrievedPoint.getMetric());
        assertEquals(point.getTags(), retrievedPoint.getTags());
    }

    @Test
    public void testWriteAndQueryBatch() throws IOException {
        // Create test points
        List<TimeseriesPoint> points = Arrays.asList(
            new TimeseriesPoint(1640995200000L, 85.5, "cpu.usage", Map.of("host", "server1")),
            new TimeseriesPoint(1640995260000L, 87.2, "cpu.usage", Map.of("host", "server1")),
            new TimeseriesPoint(1640995320000L, 83.1, "cpu.usage", Map.of("host", "server1"))
        );

        // Write points in batch
        timeseriesService.writeBatch(points);

        // Query for the points
        QueryRequest request = new QueryRequest(
            "cpu.usage",
            1640995200000L,
            1640995400000L,
            Map.of("host", "server1"),
            1000
        );

        QueryResponse response = timeseriesService.query(request);

        // Verify results
        assertNotNull(response);
        assertEquals("cpu.usage", response.getMetric());
        assertEquals(3, response.getTotalPoints());
        assertFalse(response.isTruncated());
        assertEquals(3, response.getPoints().size());

        // Verify points are sorted by timestamp
        List<TimeseriesPoint> retrievedPoints = response.getPoints();
        for (int i = 1; i < retrievedPoints.size(); i++) {
            assertTrue(retrievedPoints.get(i).getTimestamp() >= retrievedPoints.get(i-1).getTimestamp());
        }
    }

    @Test
    public void testQueryWithLimit() throws IOException {
        // Create test points
        List<TimeseriesPoint> points = Arrays.asList(
            new TimeseriesPoint(1640995200000L, 85.5, "cpu.usage"),
            new TimeseriesPoint(1640995260000L, 87.2, "cpu.usage"),
            new TimeseriesPoint(1640995320000L, 83.1, "cpu.usage"),
            new TimeseriesPoint(1640995380000L, 89.0, "cpu.usage"),
            new TimeseriesPoint(1640995440000L, 82.3, "cpu.usage")
        );

        // Write points
        timeseriesService.writeBatch(points);

        // Query with limit
        QueryRequest request = new QueryRequest(
            "cpu.usage",
            1640995200000L,
            1640995500000L,
            null,
            3 // limit to 3 points
        );

        QueryResponse response = timeseriesService.query(request);

        // Verify results
        assertNotNull(response);
        assertEquals(3, response.getPoints().size());
        assertTrue(response.isTruncated());
    }

    @Test
    public void testQueryRange() throws IOException {
        // Create test points with different metrics
        List<TimeseriesPoint> points = Arrays.asList(
            new TimeseriesPoint(1640995200000L, 85.5, "cpu.usage"),
            new TimeseriesPoint(1640995260000L, 1024.0, "memory.usage"),
            new TimeseriesPoint(1640995320000L, 83.1, "cpu.usage"),
            new TimeseriesPoint(1640995380000L, 2048.0, "memory.usage")
        );

        // Write points
        timeseriesService.writeBatch(points);

        // Query all metrics in range
        List<TimeseriesPoint> results = timeseriesService.queryRange(1640995200000L, 1640995400000L);

        // Verify results
        assertNotNull(results);
        assertEquals(4, results.size());

        // Verify all points are in the time range
        for (TimeseriesPoint point : results) {
            assertTrue(point.getTimestamp() >= 1640995200000L);
            assertTrue(point.getTimestamp() <= 1640995400000L);
        }
    }

    @Test
    public void testHealthCheck() {
        // Test health check
        boolean healthy = timeseriesService.isHealthy();
        assertTrue(healthy);
    }

    @Test
    public void testStats() {
        // Test getting stats
        var stats = timeseriesService.getClusterStats();
        assertNotNull(stats);
        assertTrue(stats.getReplicatedWrites() >= 0);
        assertTrue(stats.getReplicatedReads() >= 0);
        assertTrue(stats.getReplicationSuccessRate() >= 0.0);
    }

    @Test
    public void testPointLookupExactMatch() throws IOException {
        TimeseriesPoint point = new TimeseriesPoint(1640995200000L, 85.5, "cpu.usage", Map.of("host", "server1"));
        when(lsmTree.pointLookup(eq("cpu.usage"), anyMap(), eq(1640995200000L))).thenReturn(point);
        TimeseriesPoint result = timeseriesService.pointLookup("cpu.usage", Map.of("host", "server1"), 1640995200000L);
        assertNotNull(result);
        assertEquals(point.getMetric(), result.getMetric());
        assertEquals(point.getTimestamp(), result.getTimestamp());
        assertEquals(point.getTags(), result.getTags());
    }

    @Test
    public void testPointLookupNotFound() throws IOException {
        when(lsmTree.pointLookup(eq("cpu.usage"), anyMap(), eq(1640995200000L))).thenReturn(null);
        TimeseriesPoint result = timeseriesService.pointLookup("cpu.usage", Map.of("host", "server1"), 1640995200000L);
        assertNull(result);
    }

    @Test
    public void testRangeQueryWithTags() {
        TimeseriesPoint p1 = new TimeseriesPoint(1640995200000L, 85.5, "cpu.usage", Map.of("host", "server1"));
        TimeseriesPoint p2 = new TimeseriesPoint(1640995260000L, 87.2, "cpu.usage", Map.of("host", "server2"));
        List<TimeseriesPoint> all = List.of(p1, p2);
        when(lsmTree.readRange(eq("cpu.usage"), anyLong(), anyLong())).thenReturn(all);
        List<TimeseriesPoint> filtered = timeseriesService.rangeQuery("cpu.usage", 1640995200000L, 1640995300000L, Map.of("host", "server1"));
        assertEquals(1, filtered.size());
        assertEquals("server1", filtered.get(0).getTags().get("host"));
    }
} 