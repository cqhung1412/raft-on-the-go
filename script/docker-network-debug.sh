#!/bin/bash
# docker-network-debug.sh - Shows the current network partition status for Raft nodes

# Get all container names starting with raft-node
NODES=$(docker ps --filter "name=raft-node" --format "{{.Names}}")

if [ -z "$NODES" ]; then
  echo "No Raft nodes found running. Start the cluster first."
  exit 1
fi

echo "Current network partition status:"
echo "================================="

# Show iptables rules for each node
for node in $NODES; do
  echo "Node: $node"
  echo "-----------"
  
  # Display iptables OUTPUT chain rules
  docker exec $node iptables -L OUTPUT -v
  
  echo ""
done

# Test connectivity between nodes
echo "Connectivity test:"
echo "=================="

for source in $NODES; do
  for target in $NODES; do
    if [ "$source" != "$target" ]; then
      echo -n "Testing $source -> $target: "
      
      # Try to ping the target node (with timeout to avoid hanging)
      if docker exec $source ping -c 1 -W 1 $target >/dev/null 2>&1; then
        echo "CONNECTED"
      else
        echo "BLOCKED"
      fi
    fi
  done
done