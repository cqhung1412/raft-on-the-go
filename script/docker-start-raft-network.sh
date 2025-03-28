#!/bin/bash
# docker-start-raft-network.sh - Starts the Raft network using Docker Compose

echo "Starting Raft network with Docker Compose..."

# Navigate to the project root directory
cd "$(dirname "$0")/.." || exit

# Build image first
docker build -t raft-on-the-go-image:latest .

# Start the Docker containers
docker-compose up -d

echo "Waiting for nodes to initialize..."
sleep 5

# Display container status
docker-compose ps

# Verify connectivity between containers
echo -e "\nVerifying container connectivity..."
docker exec raft-node1 ping -c 1 node2
docker exec raft-node1 ping -c 1 node3

echo -e "\nRaft network started successfully!"
echo -e "Use the following commands to interact with the cluster:\n"
echo "- View node status: curl localhost:600X/inspect | jq (X is the node number 1-5)"
echo "- Create network partition: ./script/docker-create-partition.sh '1,2:3,4,5'"
echo "- Debug network connections: ./script/docker-network-debug.sh"
echo "- Restore network connections: ./script/docker-restore-connections.sh"
echo "- Stop cluster: docker-compose down"
echo "- View logs: docker-compose logs -f"