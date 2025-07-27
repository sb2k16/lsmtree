package com.lsmtree.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a query request for timeseries data.
 */
public class QueryRequest {
    
    private final String metric;
    private final long startTime;
    private final long endTime;
    private final Map<String, String> tags;
    private final int limit;

    @JsonCreator
    public QueryRequest(
            @JsonProperty("metric") String metric,
            @JsonProperty("start") long startTime,
            @JsonProperty("end") long endTime,
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("limit") Integer limit) {
        this.metric = Objects.requireNonNull(metric, "Metric cannot be null");
        this.startTime = startTime;
        this.endTime = endTime;
        this.tags = tags;
        this.limit = limit != null ? limit : 1000; // Default limit
        
        if (startTime >= endTime) {
            throw new IllegalArgumentException("Start time must be before end time");
        }
    }

    public QueryRequest(String metric, long startTime, long endTime) {
        this(metric, startTime, endTime, null, null);
    }

    public String getMetric() {
        return metric;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public int getLimit() {
        return limit;
    }

    /**
     * Checks if this query matches the given timeseries point.
     */
    public boolean matches(TimeseriesPoint point) {
        // Check metric
        if (!metric.equals(point.getMetric())) {
            return false;
        }
        
        // Check time range
        if (point.getTimestamp() < startTime || point.getTimestamp() > endTime) {
            return false;
        }
        
        // Check tags if specified
        if (tags != null && !tags.isEmpty()) {
            Map<String, String> pointTags = point.getTags();
            for (Map.Entry<String, String> requiredTag : tags.entrySet()) {
                String pointTagValue = pointTags.get(requiredTag.getKey());
                if (!Objects.equals(requiredTag.getValue(), pointTagValue)) {
                    return false;
                }
            }
        }
        
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        QueryRequest that = (QueryRequest) o;
        return startTime == that.startTime &&
               endTime == that.endTime &&
               limit == that.limit &&
               Objects.equals(metric, that.metric) &&
               Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metric, startTime, endTime, tags, limit);
    }

    @Override
    public String toString() {
        return String.format("QueryRequest{metric='%s', startTime=%d, endTime=%d, tags=%s, limit=%d}",
                metric, startTime, endTime, tags, limit);
    }
} 