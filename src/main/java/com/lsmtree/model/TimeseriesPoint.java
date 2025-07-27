package com.lsmtree.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single timeseries data point.
 * This is the core data structure that will be stored in the LSM tree.
 */
public class TimeseriesPoint implements Comparable<TimeseriesPoint> {
    
    private final long timestamp;
    private final double value;
    private final String metric;
    private final Map<String, String> tags;

    @JsonCreator
    public TimeseriesPoint(
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("value") double value,
            @JsonProperty("metric") String metric,
            @JsonProperty("tags") Map<String, String> tags) {
        this.timestamp = timestamp;
        this.value = value;
        this.metric = Objects.requireNonNull(metric, "Metric cannot be null");
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
    }

    public TimeseriesPoint(long timestamp, double value, String metric) {
        this(timestamp, value, metric, null);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }

    /**
     * Creates a composite key for indexing purposes.
     * Format: metric_name:tag1=value1,tag2=value2
     */
    public String getCompositeKey() {
        if (tags.isEmpty()) {
            return metric;
        }
        
        StringBuilder sb = new StringBuilder(metric);
        sb.append(":");
        
        tags.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                if (sb.length() > metric.length() + 1) {
                    sb.append(",");
                }
                sb.append(entry.getKey()).append("=").append(entry.getValue());
            });
        
        return sb.toString();
    }

    /**
     * Natural ordering by timestamp for efficient range queries.
     */
    @Override
    public int compareTo(TimeseriesPoint other) {
        int timestampComparison = Long.compare(this.timestamp, other.timestamp);
        if (timestampComparison != 0) {
            return timestampComparison;
        }
        
        // If timestamps are equal, compare by composite key for deterministic ordering
        return this.getCompositeKey().compareTo(other.getCompositeKey());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        TimeseriesPoint that = (TimeseriesPoint) o;
        return timestamp == that.timestamp &&
               Double.compare(that.value, value) == 0 &&
               Objects.equals(metric, that.metric) &&
               Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, value, metric, tags);
    }

    @Override
    public String toString() {
        return String.format("TimeseriesPoint{timestamp=%d, value=%f, metric='%s', tags=%s}",
                timestamp, value, metric, tags);
    }

    /**
     * Calculates the approximate memory size of this point in bytes.
     * Used for MemTable size management.
     */
    public int getApproximateSize() {
        int size = 8 + 8 + 4; // timestamp (long) + value (double) + object overhead
        size += metric.length() * 2; // metric string (UTF-16)
        
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            size += (tag.getKey().length() + tag.getValue().length()) * 2; // UTF-16
            size += 32; // map entry overhead
        }
        
        return size;
    }

    public static TimeseriesPoint fromString(String s) {
        // Example: TimeseriesPoint{timestamp=123, value=45.6, metric='cpu', tags={host=server1,region=us}}
        try {
            String body = s.substring(s.indexOf("{") + 1, s.lastIndexOf("}"));
            String[] parts = body.split(", (?=[a-zA-Z]+=)");
            long timestamp = 0;
            double value = 0;
            String metric = null;
            Map<String, String> tags = new HashMap<>();
            for (String part : parts) {
                String[] kv = part.split("=", 2);
                String key = kv[0].trim();
                String val = kv[1].trim();
                switch (key) {
                    case "timestamp":
                        timestamp = Long.parseLong(val);
                        break;
                    case "value":
                        value = Double.parseDouble(val);
                        break;
                    case "metric":
                        metric = val.replace("'", "");
                        break;
                    case "tags":
                        if (val.equals("{}")) break;
                        String tagsBody = val.substring(1, val.length() - 1); // remove {}
                        if (!tagsBody.isEmpty()) {
                            for (String tag : tagsBody.split(",")) {
                                String[] tagKv = tag.split("=");
                                if (tagKv.length == 2) {
                                    tags.put(tagKv[0].trim(), tagKv[1].trim());
                                }
                            }
                        }
                        break;
                }
            }
            return new TimeseriesPoint(timestamp, value, metric, tags);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse TimeseriesPoint from string: " + s, e);
        }
    }
} 