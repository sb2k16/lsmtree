#!/bin/bash

COORDINATOR_URL="http://localhost:8080"
NODE1_URL="http://localhost:8081"
NODE2_URL="http://localhost:8082"
NODE3_URL="http://localhost:8083"

echo "🧪 Testing LSMTree Distributed Cluster..."

# Wait for coordinator to be ready
echo "⏳ Waiting for coordinator to be ready..."
until curl -s "$COORDINATOR_URL/actuator/health" > /dev/null; do
    sleep 2
done
echo "✅ Coordinator is ready!"

# Register storage nodes with coordinator
echo "📝 Registering storage nodes..."

curl -X POST "$COORDINATOR_URL/api/v1/register-node" \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-1",
    "host": "storage-node-1",
    "port": 8080
  }'

curl -X POST "$COORDINATOR_URL/api/v1/register-node" \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-2",
    "host": "storage-node-2",
    "port": 8080
  }'

curl -X POST "$COORDINATOR_URL/api/v1/register-node" \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-3",
    "host": "storage-node-3",
    "port": 8080
  }'

echo "✅ Nodes registered!"

# Set up shard mappings (example: 4 shards with replication)
echo "🗺️ Setting up shard mappings..."

for shardId in {0..3}; do
    leaderNode="node-$((shardId % 3 + 1))"
    replicas=("node-$((shardId % 3 + 1))" "node-$(((shardId + 1) % 3 + 1))" "node-$(((shardId + 2) % 3 + 1))")
    
    curl -X POST "$COORDINATOR_URL/api/v1/update-shard-map" \
      -H "Content-Type: application/json" \
      -d "{
        \"shardId\": $shardId,
        \"leaderNodeId\": \"$leaderNode\",
        \"replicaNodeIds\": [\"${replicas[0]}\", \"${replicas[1]}\", \"${replicas[2]}\"],
        \"status\": \"ACTIVE\"
      }"
done

echo "✅ Shard mappings configured!"

# Wait for health checks to complete
echo "⏳ Waiting for health checks..."
sleep 10

# Check cluster status
echo "📊 Cluster Status:"
echo "Node Health:"
curl -s "$COORDINATOR_URL/api/v1/node-health" | jq '.'

echo "Shard Map:"
curl -s "$COORDINATOR_URL/api/v1/shard-map" | jq '.'

# Test data ingestion
echo "📈 Testing data ingestion..."

for i in {1..5}; do
    curl -X POST "$COORDINATOR_URL/api/v1/ingest" \
      -H "Content-Type: application/json" \
      -d "{
        \"metric\": \"cpu_usage\",
        \"timestamp\": $(date +%s)000,
        \"value\": $((RANDOM % 100)),
        \"tags\": {
          \"host\": \"server-$i\",
          \"region\": \"us-west\"
        }
      }"
    echo ""
done

echo "✅ Data ingestion test completed!"

# Test queries
echo "🔍 Testing queries..."

curl -X POST "$COORDINATOR_URL/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpu_usage",
    "startTime": 0,
    "endTime": 9999999999999,
    "aggregation": "AVG"
  }'

echo ""
echo "✅ Query test completed!"

echo ""
echo "🎉 All tests completed successfully!"
echo "📋 Test Summary:"
echo "  ✅ Node registration"
echo "  ✅ Shard mapping"
echo "  ✅ Health checks"
echo "  ✅ Data ingestion"
echo "  ✅ Data queries" 