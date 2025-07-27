# LSM Tree Timeseries Database: Architecture, Design, and Implementation

---

## Chapter 1: What is an LSM Tree?

A **Log-Structured Merge Tree (LSM Tree)** is a write-optimized data structure designed to handle high-throughput insertions and efficient range queries. Unlike traditional B-trees, which update data in place, LSM trees buffer writes in memory and periodically flush them to disk in large, sequential operations. This approach minimizes random disk I/O and is especially well-suited for workloads where write performance is critical, such as timeseries databases, logging systems, and event stores.

LSM trees are composed of multiple components: an in-memory buffer (MemTable), a write-ahead log (WAL) for durability, and a series of immutable, sorted files on disk (SSTables). Over time, these SSTables are merged in the background through a process called compaction, which keeps read performance high and storage usage efficient.

**Diagram:**
```
graph TD;
  A[Client Write] --> B(MemTable - In Memory);
  B --> C{Flush Trigger};
  C --> D[SSTable - On Disk];
  D --> E[Compaction];
  E --> D;
```

---

## Chapter 2: How Does an LSM Tree Work?

### Write Path
When a client writes data, the LSM tree first appends the operation to a write-ahead log (WAL) to ensure durability in case of a crash. The data is then inserted into the MemTable, an in-memory, sorted structure. As the MemTable grows, it eventually reaches a configured size or point count threshold. At this point, the MemTable is frozen and flushed to disk as a new SSTable file. This flush is a sequential write, which is much faster than random I/O.

### Read Path
Reads in an LSM tree must check multiple locations: the active MemTable, any immutable MemTables waiting to be flushed, and all on-disk SSTables. To avoid unnecessary disk reads, each SSTable maintains a Bloom filter, which can quickly determine if a key is definitely not present in that file. If the Bloom filter says "maybe present," the SSTable is scanned for the key.

### Compaction (Background Process)
Over time, the number of SSTables grows, which can degrade read performance. To address this, LSM trees perform **compaction**: a background process that merges multiple SSTables into fewer, larger files. Compaction eliminates duplicate or obsolete entries, reclaims space, and keeps the number of files manageable. This process is crucial for maintaining both read and write performance as the dataset grows.

**Code Snippet: Flushing MemTable to SSTable**
```java
private void flushMemTable(MemTable memTable) throws IOException {
    List<TimeseriesPoint> points = memTable.getAllPoints();
    File dataFile = ...;
    File bloomFile = ...;
    SSTable sstable = SSTable.writeToDisk(points, dataFile, bloomFile, config.getBloomFilterFalsePositiveRate());
    ssTables.add(sstable);
    memTable.markFlushed();
}
```

---

## Chapter 3: Bloom Filter — What, Why, and How

A **Bloom filter** is a probabilistic, space-efficient data structure that allows for fast set membership tests. In the context of LSM trees, each SSTable maintains a Bloom filter for the keys it contains. When a query is issued, the Bloom filter is checked before reading the SSTable from disk. If the filter says "not present," the SSTable is skipped entirely, saving expensive disk I/O. If it says "maybe present," the SSTable is scanned for the key.

Bloom filters are highly efficient because they use multiple hash functions and a bit array to represent set membership. They guarantee no false negatives (if the filter says "not present," the key is definitely not there), but may have false positives (the filter says "maybe present" when the key is not actually present). The false positive rate can be tuned based on the size of the filter and the number of hash functions.

**How it helps in LSM Trees:**
- Reduces the number of disk reads for point lookups.
- Keeps read latency low even as the number of SSTables grows.
- Allows the system to scale to large datasets without sacrificing query performance.

**Diagram:**
```
graph TD;
  Q[Query Key] -->|Check Bloom Filter| B{Might Exist?};
  B -- No --> S[Skip SSTable];
  B -- Yes --> R[Read SSTable from Disk];
```

**Code Snippet: Using Bloom Filter**
```java
for (SSTable sstable : ssTables) {
    if (sstable.mightContain(compositeKey)) {
        // Read from disk
    }
}
```

---

## Chapter 4: Why LSM Trees are Good for Timeseries Databases

Timeseries databases require high write throughput, efficient range queries, and the ability to handle large volumes of data. LSM trees are ideal for this use case because:

- **Write Optimization:** Writes are batched in memory and flushed to disk in large, sequential operations, minimizing random I/O and maximizing throughput.
- **Efficient Range Queries:** Data is stored in sorted order, making it easy to scan for points within a time range.
- **Space Efficiency:** Compaction eliminates obsolete or duplicate entries, reducing storage overhead.
- **Scalability:** LSM trees can be sharded and replicated across multiple nodes, supporting horizontal scaling and high availability.
- **Durability:** The use of a WAL ensures that no data is lost in the event of a crash.

---

## Chapter 5: LSM Tree in a Timeseries DB Use Case

In a timeseries database, each data point typically consists of a metric name, timestamp, value, and a set of tags (key-value pairs for dimensions like host, region, etc.). LSM trees are used to store these points efficiently, supporting both high ingest rates and fast queries.

- **Sharding:** Data is partitioned across nodes based on metric name and tags, allowing the system to scale horizontally.
- **Replication:** Each shard is replicated to multiple nodes for fault tolerance. Leader election ensures that only one node accepts writes for a given shard at any time.
- **Query Routing:** A source coordinator service maintains the shard map and routes queries to the appropriate shard leader.

**Diagram:**
```
graph TD;
  C[Client] --> SC[Source Coordinator];
  SC -->|Route by Shard| N1[Storage Node 1];
  SC --> N2[Storage Node 2];
  SC --> N3[Storage Node 3];
```

---

## Chapter 6: Write Path in Detail

The write path in a distributed LSM tree-based timeseries database involves several steps to ensure durability, consistency, and high availability:

1. **Client sends write to the source coordinator.** The coordinator determines the correct shard for the metric and tags, then routes the request to the leader node for that shard.
2. **Leader node writes to its MemTable and WAL.** The write is acknowledged in memory and logged for durability.
3. **Replication manager propagates the write to follower nodes.** Depending on the configured consistency level (ONE, QUORUM, ALL), the leader waits for acknowledgments from a subset or all replicas before confirming the write to the client.
4. **MemTable flushes to SSTable when full.** The in-memory data is written to disk as a new SSTable, and a Bloom filter is built for fast future lookups.
5. **Compaction runs in the background.** Old SSTables are merged, and obsolete data is removed, keeping the system performant.

**Code Snippet: Write API**
```java
@PostMapping("/write")
public ResponseEntity<?> writePoint(@RequestBody TimeseriesPoint point) {
    timeseriesService.writePoint(point);
    return ResponseEntity.ok(...);
}
```

---

## Chapter 7: Replication and Consensus

Replication and consensus are critical for ensuring data durability and availability in a distributed timeseries database.

- **Replication:**
  - The system uses a leader-follower model, where each shard has a single leader responsible for accepting writes and replicating them to followers.
  - The replication manager tracks the status of each follower and ensures that writes are propagated according to the configured consistency level.
  - Asynchronous and synchronous replication modes are supported, allowing a trade-off between latency and durability.

- **Consensus (Raft):**
  - The Raft protocol is used for leader election and log replication. Only the leader can accept writes; followers replicate the leader's log.
  - If the leader fails, a new leader is elected, and the system continues to operate without data loss.
  - Raft ensures that all nodes in a shard agree on the order of writes, providing strong consistency guarantees.

**Diagram:**
```
graph TD;
  L[Leader Node] --> F1[Follower 1];
  L --> F2[Follower 2];
  L -.-> C[Coordinator];
```

---

## Chapter 8: Query Path, Scaling, and Performance

### Query Path
When a client issues a query, the request is routed to the appropriate shard leader by the source coordinator. The leader checks its MemTable and any immutable MemTables for matching points. For on-disk SSTables, the Bloom filter is checked first to avoid unnecessary disk reads. If the filter indicates the key might be present, the SSTable is scanned for matching points. Results are aggregated and returned to the client.

### Compaction: How It Works and Best Practices
Compaction is a background process that merges multiple SSTables into fewer, larger files. This process:
- Eliminates duplicate and obsolete entries (e.g., overwritten or deleted points).
- Reduces the number of files, improving read performance.
- Reclaims disk space by removing stale data.

**Compaction Strategies:**
- **Size-Tiered Compaction:** New SSTables are merged when a certain number of similarly sized files accumulate. This approach is simple and provides good write throughput but can lead to read amplification (the same key may exist in multiple files).
- **Leveled Compaction:** SSTables are organized into levels. Each level has a size limit, and files are merged into the next level when the limit is reached. This approach reduces read amplification and keeps the number of files per level small, but requires more write amplification (more data is rewritten during compaction).

**Best Practices for Compaction:**
- Tune compaction frequency and thresholds based on workload (write-heavy vs. read-heavy).
- Monitor disk usage and compaction lag to avoid performance bottlenecks.
- Use leveled compaction for workloads with high read requirements and size-tiered for write-heavy scenarios.
- Consider parallelizing compaction across shards or nodes to maximize throughput.

**Code Snippet: Range Query**
```java
public List<TimeseriesPoint> readRange(String metric, long startTime, long endTime) {
    ...
    for (SSTable sstable : ssTables) {
        if (sstable.mightContain(metric)) {
            results.addAll(sstable.rangeQuery(metric, startTime, endTime));
        }
    }
    ...
}
```

### Scaling for Large Query Volume
- **Horizontal Scaling:** Add more shards and nodes to distribute query load.
- **Caching:** Use in-memory or distributed caches for frequently accessed data.
- **Server-Side Optimizations:** Implement parallel reads, prefetching, and query batching to maximize throughput.
- **Compaction Tuning:** Adjust compaction settings to balance write and read performance.

---

## Chapter 9: Anomaly Detection on Metrics

Anomaly detection is a valuable feature for timeseries databases, enabling users to identify unusual patterns or outliers in their data. There are several approaches to implementing anomaly detection:

- **Statistical Methods:** Use techniques like moving averages, z-scores, or seasonal decomposition to flag points that deviate significantly from expected values.
- **Machine Learning:** Train models (e.g., autoencoders, isolation forests) on historical data to detect anomalies in real time.
- **Integration:** Anomaly detection can be run as a background job, on data ingest, or as part of query processing. Detected anomalies can trigger alerts or be visualized in dashboards.

**Example:**
```java
public boolean isAnomaly(List<Double> values, double threshold) {
    double mean = ...;
    double stddev = ...;
    double latest = values.get(values.size() - 1);
    return Math.abs(latest - mean) > threshold * stddev;
}
```

**Visualization:**
- Plot metric values over time and highlight points that are flagged as anomalies.

**Diagram:**
```
graph TD;
  S[Stream of Points] --> D[Anomaly Detector];
  D -->|Alert| U[User];
```

---

# End of Book 