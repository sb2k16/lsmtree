package com.lsmtree.coordinator;

public class QueryRequest {
    private String metric;
    private long startTime;
    private long endTime;

    public QueryRequest() {}

    public QueryRequest(String metric, long startTime, long endTime) {
        this.metric = metric;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getMetric() { return metric; }
    public void setMetric(String metric) { this.metric = metric; }
    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }
} 