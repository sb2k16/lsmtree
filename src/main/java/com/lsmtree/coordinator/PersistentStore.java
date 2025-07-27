package com.lsmtree.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class PersistentStore {
    @Value("${coordinator.storage.directory:./coordinator-data}")
    private String storageDirectory;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final Map<Integer, ShardInfo> shardMap = new ConcurrentHashMap<>();

    private File nodesFile;
    private File shardMapFile;

    @PostConstruct
    public void init() throws IOException {
        File dir = new File(storageDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        nodesFile = new File(dir, "nodes.json");
        shardMapFile = new File(dir, "shard-map.json");
        
        loadData();
    }

    public void saveNodes(Collection<NodeInfo> nodes) throws IOException {
        objectMapper.writeValue(nodesFile, nodes);
    }

    public void saveShardMap(Map<Integer, ShardInfo> shardMap) throws IOException {
        objectMapper.writeValue(shardMapFile, shardMap);
    }

    public Collection<NodeInfo> loadNodes() throws IOException {
        if (nodesFile.exists()) {
            return objectMapper.readValue(nodesFile, new TypeReference<Collection<NodeInfo>>() {});
        }
        return nodes.values();
    }

    public Map<Integer, ShardInfo> loadShardMap() throws IOException {
        if (shardMapFile.exists()) {
            return objectMapper.readValue(shardMapFile, new TypeReference<Map<Integer, ShardInfo>>() {});
        }
        return shardMap;
    }

    private void loadData() throws IOException {
        if (nodesFile.exists()) {
            Collection<NodeInfo> loadedNodes = loadNodes();
            nodes.clear();
            for (NodeInfo node : loadedNodes) {
                nodes.put(node.getNodeId(), node);
            }
        }
        
        if (shardMapFile.exists()) {
            Map<Integer, ShardInfo> loadedShardMap = loadShardMap();
            shardMap.clear();
            shardMap.putAll(loadedShardMap);
        }
    }
} 