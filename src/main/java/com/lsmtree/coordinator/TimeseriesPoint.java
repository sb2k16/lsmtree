package com.lsmtree.coordinator;

import java.util.Map;

public class TimeseriesPoint {
    private long timestamp;
    private double value;
    private String metric;
    private Map<String, String> tags;

    public TimeseriesPoint() {}

    public TimeseriesPoint(long timestamp, double value, String metric, Map<String, String> tags) {
        this.timestamp = timestamp;
        this.value = value;
        this.metric = metric;
        this.tags = tags;
    }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }
    public String getMetric() { return metric; }
    public void setMetric(String metric) { this.metric = metric; }
    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = tags; }
} 