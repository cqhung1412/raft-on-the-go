#!/bin/bash
# docker-restore-connections.sh - Removes network partitions between Raft nodes

# Get all container names starting with raft-node
NODES=$(docker ps --filter "name=raft-node" --format "{{.Names}}")

if [ -z "$NODES" ]; then
  echo "No Raft nodes found running. Start the cluster first."
  exit 1
fi

echo "Restoring network connections between Raft nodes..."

# Flush iptables rules for each node
for node in $NODES; do
  echo "Flushing iptables rules for $node"
  docker exec $node iptables -F OUTPUT
done

echo "Network connections restored successfully"