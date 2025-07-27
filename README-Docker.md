# LSMTree Distributed Cluster - Docker Setup

This guide shows how to run a complete distributed LSMTree cluster with 3 storage nodes and 1 coordinator using Docker.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Coordinator   │    │  Storage Node 1 │    │  Storage Node 2 │
│   (Port 8080)   │    │   (Port 8081)   │    │   (Port 8082)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Storage Node 3 │
                    │   (Port 8083)   │
                    └─────────────────┘
```

## Prerequisites

- Docker and Docker Compose installed
- `jq` for JSON formatting (optional, for better output)

## Quick Start

### 1. Start the Cluster

```bash
# Start all services
./start-cluster.sh

# Or manually:
docker-compose up -d --build
```

### 2. Test the Cluster

```bash
# Run comprehensive tests
./test-cluster.sh
```

## Manual Testing

### 1. Check Service Health

```bash
# Check if all services are running
docker-compose ps

# Check coordinator health
curl http://localhost:8080/actuator/health

# Check storage nodes health
curl http://localhost:8081/actuator/health
curl http://localhost:8082/actuator/health
curl http://localhost:8083/actuator/health
```

### 2. Register Nodes with Coordinator

```bash
# Register storage nodes
curl -X POST http://localhost:8080/api/v1/register-node \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-1",
    "host": "storage-node-1",
    "port": 8080
  }'

curl -X POST http://localhost:8080/api/v1/register-node \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-2",
    "host": "storage-node-2",
    "port": 8080
  }'

curl -X POST http://localhost:8080/api/v1/register-node \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "node-3",
    "host": "storage-node-3",
    "port": 8080
  }'
```

### 3. Configure Shard Mappings

```bash
# Set up shard 0 with node-1 as leader
curl -X POST http://localhost:8080/api/v1/update-shard-map \
  -H "Content-Type: application/json" \
  -d '{
    "shardId": 0,
    "leaderNodeId": "node-1",
    "replicaNodeIds": ["node-1", "node-2", "node-3"],
    "status": "ACTIVE"
  }'

# Set up shard 1 with node-2 as leader
curl -X POST http://localhost:8080/api/v1/update-shard-map \
  -H "Content-Type: application/json" \
  -d '{
    "shardId": 1,
    "leaderNodeId": "node-2",
    "replicaNodeIds": ["node-2", "node-3", "node-1"],
    "status": "ACTIVE"
  }'

# Set up shard 2 with node-3 as leader
curl -X POST http://localhost:8080/api/v1/update-shard-map \
  -H "Content-Type: application/json" \
  -d '{
    "shardId": 2,
    "leaderNodeId": "node-3",
    "replicaNodeIds": ["node-3", "node-1", "node-2"],
    "status": "ACTIVE"
  }'
```

### 4. Check Cluster Status

```bash
# View node health
curl http://localhost:8080/api/v1/node-health | jq '.'

# View shard map
curl http://localhost:8080/api/v1/shard-map | jq '.'
```

### 5. Test Data Operations

```bash
# Ingest timeseries data
curl -X POST http://localhost:8080/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpu_usage",
    "timestamp": 1640995200000,
    "value": 75.5,
    "tags": {
      "host": "server-1",
      "region": "us-west"
    }
  }'

# Query timeseries data
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpu_usage",
    "startTime": 1640995200000,
    "endTime": 1640995260000,
    "aggregation": "AVG"
  }'
```

## Service URLs

| Service | URL | Description |
|---------|-----|-------------|
| Coordinator | http://localhost:8080 | Central routing proxy |
| Storage Node 1 | http://localhost:8081 | LSMTree storage node |
| Storage Node 2 | http://localhost:8082 | LSMTree storage node |
| Storage Node 3 | http://localhost:8083 | LSMTree storage node |

## Coordinator Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/ingest` | POST | Ingest timeseries data |
| `/api/v1/query` | POST | Query timeseries data |
| `/api/v1/register-node` | POST | Register a storage node |
| `/api/v1/unregister-node` | POST | Unregister a storage node |
| `/api/v1/update-shard-map` | POST | Update shard mapping |
| `/api/v1/shard-map` | GET | Get current shard map |
| `/api/v1/node-health` | GET | Get node health status |

## Monitoring

### View Logs

```bash
# View coordinator logs
docker-compose logs -f coordinator

# View storage node logs
docker-compose logs -f storage-node-1
docker-compose logs -f storage-node-2
docker-compose logs -f storage-node-3

# View all logs
docker-compose logs -f
```

### Check Resource Usage

```bash
# Check container resource usage
docker stats
```

## Troubleshooting

### Common Issues

1. **Services not starting**: Check if ports are already in use
   ```bash
   lsof -i :8080-8083
   ```

2. **Health checks failing**: Wait for services to fully start
   ```bash
   docker-compose logs coordinator
   ```

3. **Network connectivity**: Ensure containers can communicate
   ```bash
   docker network ls
   docker network inspect lsmtree_lsmtree-network
   ```

### Clean Restart

```bash
# Stop and remove everything
docker-compose down -v

# Rebuild and start
docker-compose up -d --build
```

## Stopping the Cluster

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (data will be lost)
docker-compose down -v
```

## Data Persistence

- **Storage nodes**: Data persisted in Docker volumes (`lsmtree-data-*`)
- **Coordinator**: Node registry and shard mappings persisted in `coordinator-data` volume
- **Logs**: All logs persisted in `*-logs` volumes

Data survives container restarts but is lost when volumes are removed. 