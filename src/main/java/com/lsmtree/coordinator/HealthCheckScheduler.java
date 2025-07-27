package com.lsmtree.coordinator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class HealthCheckScheduler {
    @Autowired
    private NodeRegistry nodeRegistry;
    @Autowired
    private RestTemplate restTemplate;

    @Value("${coordinator.healthcheck.interval-ms:5000}")
    private long healthCheckIntervalMs;

    @Scheduled(fixedDelayString = "${coordinator.healthcheck.interval-ms:5000}")
    public void checkNodeHealth() {
        for (NodeInfo node : nodeRegistry.getAllNodes()) {
            String url = String.format("http://%s:%d/health", node.getHost(), node.getPort());
            try {
                String resp = restTemplate.getForObject(url, String.class);
                if (resp != null && resp.contains("UP")) {
                    nodeRegistry.setNodeHealth(node.getNodeId(), NodeInfo.HealthStatus.HEALTHY);
                } else {
                    nodeRegistry.setNodeHealth(node.getNodeId(), NodeInfo.HealthStatus.UNHEALTHY);
                }
            } catch (Exception e) {
                nodeRegistry.setNodeHealth(node.getNodeId(), NodeInfo.HealthStatus.UNHEALTHY);
            }
        }
    }
} 