package com.lsmtree;

import com.lsmtree.storage.ReplicatedLSMTree;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Main Spring Boot application for the LSM Tree Timeseries Storage.
 * 
 * This application provides a high-performance, in-memory timeseries database
 * using LSM (Log-Structured Merge) tree architecture for efficient write and read operations.
 * 
 * Features:
 * - High write throughput with MemTable-based writes
 * - Write-Ahead Log (WAL) for durability
 * - Efficient range queries for timeseries data
 * - RESTful API for data ingestion and querying
 * - Configurable storage parameters
 * - Health monitoring and metrics
 */
@SpringBootApplication
@EnableConfigurationProperties
public class Application {
    
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    /**
     * Primary LSM tree bean - uses the replicated version.
     */
    @Bean
    @Primary
    public ReplicatedLSMTree replicatedLSMTree() {
        return new ReplicatedLSMTree();
    }
} 