#!/bin/bash
# docker-kill-raft-network.sh - Stops the Raft network running in Docker

echo "Stopping Raft network..."

# Navigate to the project root directory
cd "$(dirname "$0")/.." || exit

# Stop and remove containers
docker-compose down

echo "Raft network stopped successfully!"