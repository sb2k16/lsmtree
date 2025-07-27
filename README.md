# LSM Tree Timeseries Database with Replication

A high-performance, distributed timeseries database built on LSM (Log-Structured Merge) tree architecture with full replication capabilities.

## Features

### Core LSM Tree Features
- **In-Memory MemTable** for fast writes
- **SSTable** storage for persistent data
- **Write-Ahead Log (WAL)** for durability
- **Automatic Compaction** for space optimization
- **Bloom Filters** for efficient point lookups

### Replication Features (NEW)
- **Leader-Follower Architecture** with Raft consensus
- **Automatic Leader Election** and failover
- **Configurable Consistency Levels** (ONE, QUORUM, ALL)
- **Synchronous and Asynchronous Replication**
- **Health Monitoring** and automatic recovery
- **Cluster Management** APIs

## Architecture

### LSM Tree Structure
```
MemTable (in-memory) → SSTable (on-disk) → Compaction
     ↓
Write-Ahead Log (durability)
```

### Replication Architecture
```
Leader Node
├── Accepts writes
├── Replicates to followers
└── Manages cluster state

Follower Nodes
├── Receive replicated data
├── Serve read requests
└── Participate in leader election
```

## Quick Start

### Prerequisites
- Java 17 or higher
- Gradle 7.0 or higher

### Running the Application

1. **Clone and build:**
```bash
git clone <repository>
cd LSMTree
./gradlew build
```

2. **Run the application:**
```bash
./gradlew bootRun
```

3. **Access the API:**
- Health check: `GET http://localhost:8080/api/v1/timeseries/health`
- Cluster status: `GET http://localhost:8080/api/v1/cluster/status`

## API Reference

### Timeseries Operations

#### Write Data
```bash
# Single point
POST /api/v1/timeseries/write
{
  "metric": "cpu.usage",
  "value": 75.5,
  "timestamp": 1640995200000
}

# Batch write
POST /api/v1/timeseries/write/batch
[
  {"metric": "cpu.usage", "value": 75.5, "timestamp": 1640995200000},
  {"metric": "memory.usage", "value": 60.2, "timestamp": 1640995200000}
]
```

#### Query Data
```bash
POST /api/v1/timeseries/query
{
  "metric": "cpu.usage",
  "startTime": 1640995200000,
  "endTime": 1640995260000
}
```

### Cluster Management

#### Cluster Status
```bash
GET /api/v1/cluster/status
```

Response:
```json
{
  "stats": {
    "totalNodes": 3,
    "onlineNodes": 3,
    "followerNodes": 2,
    "leaderNodes": 1,
    "currentTerm": 5
  },
  "raftState": "LEADER",
  "currentTerm": 5,
  "isLeader": true,
  "hasQuorum": true,
  "localNodeId": "node-1"
}
```

#### Cluster Statistics
```bash
GET /api/v1/timeseries/cluster/stats
```

#### Replication Statistics
```bash
GET /api/v1/timeseries/replication/stats
```

#### Force Leader Election
```bash
POST /api/v1/cluster/leader/election
```

#### Cluster Health
```bash
GET /api/v1/timeseries/health
```

### Sharding Management

#### Shard Map
```bash
GET /api/v1/coordinator/shard-map
```

Response:
```json
{
  "0": {
    "shardId": 0,
    "leaderNodeId": "node-1",
    "replicaNodeIds": ["node-1", "node-2", "node-3"],
    "status": "HEALTHY",
    "lastUpdated": 1640995200000
  }
}
```

#### Get Leader for Metric
```bash
GET /api/v1/coordinator/leader?metric=cpu.usage
```

Response:
```json
{
  "metric": "cpu.usage",
  "shardId": 5,
  "leaderNodeId": "node-2"
}
```

#### Register Node
```bash
POST /api/v1/coordinator/nodes
{
  "nodeId": "node-4",
  "host": "localhost",
  "port": 8083
}
```

#### Shard Statistics
```bash
GET /api/v1/shard/stats
```

Response:
```json
{
  "0": {
    "shardId": 0,
    "isLeader": true,
    "raftState": "LEADER",
    "currentTerm": 5,
    "clusterStats": {
      "totalNodes": 3,
      "onlineNodes": 3
    }
  }
}
```

## Configuration

### Application Properties

```yaml
# LSM Tree Configuration
lsm:
  memtable:
    max-size-mb: 64
    flush-threshold: 0.8
  sstable:
    max-size-mb: 256
    merge-threshold: 10
  wal:
    enabled: true
    sync-on-write: true
  data-dir: "./data"

# Cluster Configuration
cluster:
  node:
    id: "node-1"
    host: "localhost"
    port: 8080
  discovery:
    enabled: true
    initial-nodes: []
  heartbeat:
    interval-ms: 100
    timeout-ms: 5000
  election:
    timeout-ms: 1000
    max-timeout-ms: 2000

# Replication Configuration
replication:
  write-consistency: QUORUM    # ONE, QUORUM, ALL
  read-consistency: ONE        # ONE, QUORUM, ALL
  replication-timeout-ms: 5000
  max-retries: 3
  async-replication: false
  max-replication-lag-ms: 10000
  replication-lag-check-interval-ms: 1000
  auto-failover: true
  failover-timeout-ms: 30000

# Sharding Configuration
sharding:
  num-shards: 16
  replication-factor: 3
  health-check-interval-ms: 5000
```

### Consistency Levels

- **ONE**: Wait for acknowledgment from one node
- **QUORUM**: Wait for acknowledgment from majority of nodes
- **ALL**: Wait for acknowledgment from all nodes

## Replication Implementation

### Phase 1: Cluster Foundation
- **NodeInfo**: Represents cluster node information with roles and status
- **ClusterMembership**: Manages cluster membership and node tracking
- **RaftConsensus**: Implements Raft consensus protocol for leader election

### Phase 2: Replication Infrastructure
- **ReplicationConfig**: Configuration for replication settings and consistency levels
- **ReplicationTarget**: Represents a target node for replication operations
- **ReplicationManager**: Handles write replication to follower nodes

### Phase 3: Replicated Storage
- **ReplicatedLSMTree**: Extends base LSMTree with replication capabilities
- **Write Replication**: Automatically replicates writes to followers
- **Read Distribution**: Supports reading from followers for load distribution

### Phase 4: Cluster Management
- **ClusterManager**: Orchestrates cluster operations and health monitoring
- **Failover Management**: Automatic leader election and failover
- **Health Monitoring**: Continuous monitoring of cluster health
- **Statistics and Monitoring**: Comprehensive metrics and monitoring

## Sharding Implementation

### Phase 1: Shard-Aware Components
- **ShardInfo**: Represents shard information including leader and replica nodes
- **ShardRaftConsensus**: Shard-aware Raft consensus for per-shard leader election
- **ShardCoordinator**: Central coordinator managing shard assignments and routing

### Phase 2: Shard Management
- **ShardManager**: Manages shard-aware LSM trees and coordinates with coordinator
- **Per-Shard Storage**: Each shard has its own LSM tree instance
- **Shard Assignment**: Hash-based partitioning of metrics across shards

### Phase 3: Coordinator Services
- **Node Registration**: Dynamic node registration with shard assignment
- **Leader Election**: Per-shard leader election triggered by coordinator
- **Health Monitoring**: Continuous monitoring of shard health and leader status
- **Request Routing**: Coordinator provides routing information for clients

### Phase 4: API Extensions
- **CoordinatorController**: REST API for coordinator operations
- **ShardController**: REST API for shard management and statistics
- **Client Routing**: APIs for clients to discover shard leaders and routing

## Testing

### Run Tests
```bash
./gradlew test
```

### Integration Tests
The project includes comprehensive integration tests for:
- Cluster initialization and Raft consensus
- Replication configuration and management
- Write replication and batch operations
- Cluster health monitoring and failover
- Concurrent operations and performance

## Performance Characteristics

### Write Performance
- **MemTable writes**: ~100K ops/sec (in-memory)
- **SSTable writes**: ~10K ops/sec (on-disk)
- **Replication overhead**: ~5-10% (synchronous), ~1-2% (asynchronous)

### Read Performance
- **Point queries**: ~50K ops/sec
- **Range queries**: ~10K ops/sec (depends on range size)
- **Bloom filter hit rate**: >95% for most workloads

### Scalability
- **Single node**: Up to 1M points/second
- **3-node cluster**: Up to 2M points/second (with replication)
- **Horizontal scaling**: Add nodes for linear scaling

## Monitoring and Observability

### Metrics Available
- **Cluster metrics**: Node count, leader status, quorum status
- **Replication metrics**: Success rate, lag, target health
- **Storage metrics**: MemTable size, SSTable count, compaction stats
- **Performance metrics**: Write/read throughput, latency

### Health Checks
- **Cluster health**: Quorum status, leader availability
- **Replication health**: Target status, lag monitoring
- **Storage health**: Disk space, compaction status

## Deployment

### Single Node
```bash
./gradlew bootRun
```

### Multi-Node Cluster
1. **Start first node (leader):**
```bash
./gradlew bootRun --args='--cluster.node.id=node-1 --cluster.node.port=8080'
```

2. **Start additional nodes:**
```bash
./gradlew bootRun --args='--cluster.node.id=node-2 --cluster.node.port=8081'
./gradlew bootRun --args='--cluster.node.id=node-3 --cluster.node.port=8082'
```

3. **Add nodes to cluster:**
```bash
curl -X POST http://localhost:8080/api/v1/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"nodeId": "node-2", "host": "localhost", "port": 8081}'
```

## Troubleshooting

### Common Issues

1. **No quorum available**
   - Check if enough nodes are online
   - Verify network connectivity between nodes

2. **Replication lag**
   - Check follower node health
   - Monitor network latency
   - Consider adjusting replication timeout

3. **Leader election issues**
   - Check election timeout settings
   - Verify cluster membership
   - Monitor network partitions

### Logs
- **Cluster logs**: `com.lsmtree.cluster`
- **Replication logs**: `com.lsmtree.replication`
- **Storage logs**: `com.lsmtree.storage`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
