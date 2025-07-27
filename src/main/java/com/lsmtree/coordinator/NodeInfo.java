package com.lsmtree.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NodeInfo {
    public enum HealthStatus { HEALTHY, UNHEALTHY }

    private final String nodeId;
    private final String host;
    private final int port;
    private volatile HealthStatus healthStatus = HealthStatus.HEALTHY;

    @JsonCreator
    public NodeInfo(@JsonProperty("nodeId") String nodeId, 
                   @JsonProperty("host") String host, 
                   @JsonProperty("port") int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public HealthStatus getHealthStatus() {
        return healthStatus;
    }

    public void setHealthStatus(HealthStatus healthStatus) {
        this.healthStatus = healthStatus;
    }
} 