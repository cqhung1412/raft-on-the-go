# Raft-on-the-Go

A robust implementation of the [Raft consensus algorithm](https://raft.github.io/) in Go using gRPC for communication between nodes.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Getting Started](#getting-started)
- [Usage](#usage)
  - [Managing the Cluster](#managing-the-cluster)
  - [Simulating Network Conditions](#simulating-network-conditions)
  - [Interacting with the Cluster](#interacting-with-the-cluster)
- [API Reference](#api-reference)
- [Fault Tolerance](#fault-tolerance)
- [Implementation Details](#implementation-details)

## Overview

Raft-on-the-Go is a distributed consensus implementation that provides a consistent, fault-tolerant key-value store across multiple nodes. It follows the Raft protocol for leader election, log replication, and handling network partitions.

## Features

- Complete implementation of the Raft consensus algorithm
- Leader election with term-based voting
- Log replication with consistency guarantees
- Automatic recovery of failed nodes
- Network partition tolerance
- HTTP API for cluster interaction
- Simulated network partition testing

## Getting Started

### Prerequisites

- Go 1.16+
- Protocol Buffers compiler (`protoc`)
- tmux (for multi-node testing)

### Installation

1. Clone the repository
2. Generate protocol buffer code:

   ```sh
   protoc --go_out=. --go-grpc_out=. ./proto/raft.proto
   ```

3. Make scripts executable:

   ```sh
   chmod +x ./script/*.sh
   ```

## Usage

### Managing the Cluster

**Start a single node**

```sh
go run main.go
```

**Launch a 5-node cluster using tmux**

```sh
./script/start-raft-network.sh
```

This creates a cluster of 5 nodes running on ports 5001-5005 (gRPC) and 6001-6005 (HTTP).

**Shutdown the cluster**

```sh
./script/kill-raft-network.sh
```

**Create a client to interact with the leader**

```sh
go run main.go --client --leader=<leader-port|5001> --term=<leader-term|1>
```

### Simulating Network Conditions

**Create a network partition** (requires sudo)

```sh
sudo ./script/network-partition.sh create
```

This creates two partitions:

- Partition 1: Nodes on ports 5001, 5002, 5003
- Partition 2: Nodes on ports 5004, 5005

**Restore network connectivity**

```sh
sudo ./script/network-partition.sh remove
```

### Interacting with the Cluster

**Inspect node state**

```sh
curl localhost:<http-port>/inspect
```

> Note: HTTP port = gRPC port + 1000 (e.g., gRPC port 5001 → HTTP port 6001)

**Add an entry to the distributed log** (must be sent to leader)

```sh
curl -X POST http://localhost:6001/append -H "Content-Type: application/json" -d '{
  "term": 1,
  "entries": [
    {
      "command": "key1=value1"
    }
  ]
}'
```

**Response format**

```json
{
  "term": 1,
  "success": true,
  "nextIndex": 2
}
```

**Trigger node shutdown** (gracefully stops a node)

```sh
curl -X POST http://localhost:6001/shutdown
```

## API Reference

### HTTP Endpoints

| Endpoint    | Method | Description                                       |
|-------------|--------|---------------------------------------------------|
| `/inspect`  | GET    | Returns the current state of the node             |
| `/append`   | POST   | Appends entries to the distributed log            |
| `/shutdown` | POST   | Gracefully shuts down the node                    |

### Inspect Response Example

```json
{
  "node_id": "node1",
  "current_term": 2,
  "state": "Leader",
  "log_entries": [
    {
      "index": 1,
      "term": 1,
      "command": "key1=value1"
    }
  ],
  "commit_index": 1,
  "last_applied": 1,
  "kv_store": {
    "key1": "value1"
  }
}
```

## Fault Tolerance

The implementation includes robust error handling for various failure scenarios:

1. **Node Failures**: When a follower node crashes or shuts down, the leader continues to function with the remaining nodes as long as a majority is available.

2. **Leader Failures**: If a leader fails, a new election is triggered after the election timeout, and a new leader is elected.

3. **Node Recovery**: When a failed node rejoins the cluster:
   - It automatically receives heartbeats from the current leader
   - The leader detects that the follower is behind (through the `needsSync` flag)
   - Missing log entries are sent to bring the follower up to date
   - Log consistency is verified using the `prevLogIndex` and `prevLogTerm` fields

4. **Network Partitions**: If the network is partitioned:
   - Each partition may elect its own leader (split-brain)
   - When the partition heals, the leader with the higher term prevails
   - Log inconsistencies are resolved according to the Raft protocol
   - Eventually, all nodes converge to a consistent state

## Implementation Details

This implementation follows the Raft consensus algorithm with:

- **State Management**: Each node can be in Follower, Candidate, or Leader state
- **Log Replication**: Ensures all logs across the cluster are eventually consistent
- **Safety Guarantees**: Only committed entries (replicated to a majority) are applied
- **Term-Based Elections**: Prevents multiple leaders from existing in the same term
- **Optimizations**: Fast log replication with nextIndex/matchIndex tracking

The core consensus logic is in `utils/raft.go`, while the networking and API are in `server/server.go`.
