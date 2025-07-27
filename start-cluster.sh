#!/bin/bash

echo "🚀 Starting LSMTree Distributed Cluster..."

# Build and start all services
echo "📦 Building and starting services..."
docker-compose up -d --build

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check service health
echo "🔍 Checking service health..."
docker-compose ps

echo "✅ Cluster started successfully!"
echo ""
echo "📊 Service URLs:"
echo "  Coordinator:     http://localhost:8080"
echo "  Storage Node 1:  http://localhost:8081"
echo "  Storage Node 2:  http://localhost:8082"
echo "  Storage Node 3:  http://localhost:8083"
echo ""
echo "📋 Next steps:"
echo "  1. Register nodes with coordinator"
echo "  2. Update shard mappings"
echo "  3. Test data ingestion and queries"
echo ""
echo "🔧 To view logs: docker-compose logs -f [service-name]"
echo "🛑 To stop cluster: docker-compose down" 