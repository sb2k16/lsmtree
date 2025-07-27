package com.lsmtree.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Represents a query response containing timeseries data points.
 */
public class QueryResponse {
    
    private final List<TimeseriesPoint> points;
    private final String metric;
    private final long queryTimeMs;
    private final int totalPoints;
    private final boolean truncated;

    @JsonCreator
    public QueryResponse(
            @JsonProperty("points") List<TimeseriesPoint> points,
            @JsonProperty("metric") String metric,
            @JsonProperty("queryTimeMs") long queryTimeMs,
            @JsonProperty("totalPoints") int totalPoints,
            @JsonProperty("truncated") boolean truncated) {
        this.points = Objects.requireNonNull(points, "Points cannot be null");
        this.metric = metric;
        this.queryTimeMs = queryTimeMs;
        this.totalPoints = totalPoints;
        this.truncated = truncated;
    }

    public QueryResponse(List<TimeseriesPoint> points, String metric, long queryTimeMs) {
        this(points, metric, queryTimeMs, points.size(), false);
    }

    public List<TimeseriesPoint> getPoints() {
        return points;
    }

    public String getMetric() {
        return metric;
    }

    public long getQueryTimeMs() {
        return queryTimeMs;
    }

    public int getTotalPoints() {
        return totalPoints;
    }

    public boolean isTruncated() {
        return truncated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        QueryResponse that = (QueryResponse) o;
        return queryTimeMs == that.queryTimeMs &&
               totalPoints == that.totalPoints &&
               truncated == that.truncated &&
               Objects.equals(points, that.points) &&
               Objects.equals(metric, that.metric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(points, metric, queryTimeMs, totalPoints, truncated);
    }

    @Override
    public String toString() {
        return String.format("QueryResponse{metric='%s', totalPoints=%d, queryTimeMs=%d, truncated=%s}",
                metric, totalPoints, queryTimeMs, truncated);
    }
} 