package com.lsmtree.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for the LSM tree storage engine.
 */
@Component
@ConfigurationProperties(prefix = "lsm")
public class LSMConfig {
    
    /**
     * Maximum size of MemTable in megabytes before flushing to disk.
     */
    private int memtableMaxSizeMb = 64;
    
    /**
     * Maximum number of points in MemTable before flushing.
     */
    private int memtableMaxPoints = 100000;
    
    /**
     * Enable/disable Write-Ahead Log for durability.
     */
    private boolean walEnabled = true;
    
    /**
     * Directory for storing SSTables and WAL files.
     */
    private String storageDirectory = "./data";
    
    /**
     * Interval in seconds for background compaction.
     */
    private int compactionIntervalSeconds = 300;
    
    /**
     * Maximum number of SSTables in L0 before forcing compaction.
     */
    private int maxL0Tables = 4;
    
    /**
     * Size multiplier for each level (L1 = L0 * multiplier, L2 = L1 * multiplier, etc.).
     */
    private int levelSizeMultiplier = 10;
    
    /**
     * Maximum number of concurrent compactions.
     */
    private int maxConcurrentCompactions = 2;
    
    /**
     * Enable/disable bloom filters for faster lookups.
     */
    private boolean bloomFilterEnabled = true;
    
    /**
     * Bloom filter false positive rate (0.0 to 1.0).
     */
    private double bloomFilterFalsePositiveRate = 0.01;

    // Getters and Setters
    public int getMemtableMaxSizeMb() {
        return memtableMaxSizeMb;
    }

    public void setMemtableMaxSizeMb(int memtableMaxSizeMb) {
        this.memtableMaxSizeMb = memtableMaxSizeMb;
    }

    public int getMemtableMaxPoints() {
        return memtableMaxPoints;
    }

    public void setMemtableMaxPoints(int memtableMaxPoints) {
        this.memtableMaxPoints = memtableMaxPoints;
    }

    public boolean isWalEnabled() {
        return walEnabled;
    }

    public void setWalEnabled(boolean walEnabled) {
        this.walEnabled = walEnabled;
    }

    public String getStorageDirectory() {
        return storageDirectory;
    }

    public void setStorageDirectory(String storageDirectory) {
        this.storageDirectory = storageDirectory;
    }

    public int getCompactionIntervalSeconds() {
        return compactionIntervalSeconds;
    }

    public void setCompactionIntervalSeconds(int compactionIntervalSeconds) {
        this.compactionIntervalSeconds = compactionIntervalSeconds;
    }

    public int getMaxL0Tables() {
        return maxL0Tables;
    }

    public void setMaxL0Tables(int maxL0Tables) {
        this.maxL0Tables = maxL0Tables;
    }

    public int getLevelSizeMultiplier() {
        return levelSizeMultiplier;
    }

    public void setLevelSizeMultiplier(int levelSizeMultiplier) {
        this.levelSizeMultiplier = levelSizeMultiplier;
    }

    public int getMaxConcurrentCompactions() {
        return maxConcurrentCompactions;
    }

    public void setMaxConcurrentCompactions(int maxConcurrentCompactions) {
        this.maxConcurrentCompactions = maxConcurrentCompactions;
    }

    public boolean isBloomFilterEnabled() {
        return bloomFilterEnabled;
    }

    public void setBloomFilterEnabled(boolean bloomFilterEnabled) {
        this.bloomFilterEnabled = bloomFilterEnabled;
    }

    public double getBloomFilterFalsePositiveRate() {
        return bloomFilterFalsePositiveRate;
    }

    public void setBloomFilterFalsePositiveRate(double bloomFilterFalsePositiveRate) {
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
    }

    /**
     * Get MemTable max size in bytes.
     */
    public long getMemtableMaxSizeBytes() {
        return (long) memtableMaxSizeMb * 1024 * 1024;
    }

    @Override
    public String toString() {
        return String.format("LSMConfig{memtableMaxSizeMb=%d, memtableMaxPoints=%d, walEnabled=%s, " +
                           "storageDirectory='%s', compactionIntervalSeconds=%d, maxL0Tables=%d, " +
                           "levelSizeMultiplier=%d, maxConcurrentCompactions=%d, bloomFilterEnabled=%s}",
                memtableMaxSizeMb, memtableMaxPoints, walEnabled, storageDirectory,
                compactionIntervalSeconds, maxL0Tables, levelSizeMultiplier,
                maxConcurrentCompactions, bloomFilterEnabled);
    }
} 