#!/bin/bash
# docker-create-partition.sh - Creates network partitions between Raft nodes

# Check if arguments are provided
if [ $# -lt 1 ]; then
  echo "Usage: $0 <partition_spec>"
  echo "Examples:"
  echo "  $0 '1,2:3,4,5'  # Partition between nodes 1,2 and nodes 3,4,5"
  echo "  $0 '1,2,3:4,5'  # Partition between nodes 1,2,3 and nodes 4,5"
  echo "  $0 '1:2,3,4,5'  # Isolate node 1 from all other nodes"
  exit 1
fi

PARTITION_SPEC=$(echo "$1" | tr -d ' ')  # Remove spaces
IFS=':' read -r GROUP1 GROUP2 <<< "$PARTITION_SPEC"

if [ -z "$GROUP1" ] || [ -z "$GROUP2" ]; then
  echo "Error: Partition spec must have exactly two groups separated by ':'"
  exit 1
fi

# PARTITION_SPEC=$1
# IFS=':' read -ra GROUPS <<< "$PARTITION_SPEC"

# if [ ${#GROUPS[@]} -ne 2 ]; then
#   echo "Error: Partition spec must have exactly two groups separated by ':'"
#   exit 1
# fi

# Extract nodes from each group
IFS=',' read -ra GROUP1 <<< "$GROUP1"
IFS=',' read -ra GROUP2 <<< "$GROUP2"

echo "Creating network partition between:"
echo "Group 1: ${GROUP1[*]}"
echo "Group 2: ${GROUP2[*]}"

# Create partitions by blocking traffic in both directions
for node1 in "${GROUP1[@]}"; do
  for node2 in "${GROUP2[@]}"; do
    echo "Blocking traffic between raft-node$node1 and raft-node$node2"
    
    # Block communication from node1 to node2
    docker exec raft-node$node1 iptables -A OUTPUT -d raft-node$node2 -j DROP
    
    # Block communication from node2 to node1
    docker exec raft-node$node2 iptables -A OUTPUT -d raft-node$node1 -j DROP
  done
done

echo "Network partition created successfully"