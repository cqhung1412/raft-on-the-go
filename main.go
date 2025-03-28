package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	raftpb "raft-on-the-go/proto"
	"raft-on-the-go/server"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
)

// runServer starts a Raft server node with specified node ID, port, and peers.
// It computes an HTTP inspection port by adding 1000 to the gRPC port,
// initializes a new node with the given peer addresses, and concurrently launches
// both the node's gRPC and HTTP inspection services.
func runServer(nodeID, port string, peers []string) {
	// Compute HTTP inspect port (+1000)
	grpcPort, err := strconv.Atoi(port)
	if err != nil {
		log.Fatalf("Invalid port %s", port)
	}
	inspectPort := strconv.Itoa(grpcPort + 1000)

	// Create a new node
	log.Printf("[%s] Starting process on port %s...\n", nodeID, port)
	newNode := server.NewNode(nodeID, port, peers)

	// Start the gRPC server
	go func() {
		newNode.Start()
	}()

	// Start the HTTP inspection server
	go func() {
		newNode.StartHTTP(inspectPort)
	}()

	// Wait a moment for all nodes to start up, then initialize the Raft node
	go func() {
		// Give nodes time to start their gRPC servers
		time.Sleep(3 * time.Second)
		log.Printf("[%s] All nodes should be up now, initializing Raft node", nodeID)
		newNode.InitializeRaftNode()
	}()

	// Keep the program running indefinitely
	select {}
}

// runClient connects to a Raft leader node via gRPC and sends an AppendEntries request.
//
// It establishes a gRPC connection to the leader at the given address,
// constructs an AppendRequest with a log entry that includes the specified term
// and a predefined command ("key1=value1"), and sends the request using the Raft service client.
func runClient(leaderAddress string, term int32) {
	// If no host specified, assume localhost
	if !strings.Contains(leaderAddress, ":") {
		leaderAddress = "localhost:" + leaderAddress
	}

	// Connect to the leader node via gRPC
	conn, err := grpc.Dial(leaderAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect to leader: %v", err)
	}
	defer conn.Close()

	// Create a gRPC client
	client := raftpb.NewRaftClient(conn)

	// Create AppendEntries request
	entries := []*raftpb.LogEntry{
		{
			Term:    term,
			Command: "key1=value1",
		},
	}
	req := &raftpb.AppendRequest{
		Term:    term,
		Entries: entries,
	}

	// Send AppendEntries request
	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		log.Fatalf("Error when sending AppendEntries: %v", err)
	}

	fmt.Printf("Leader response: Term=%d, Success=%v\n", resp.Term, resp.Success)
}

// autoDetectServer starts a Raft server node on the first available port within the range 5001 to 5005.
// It sequentially checks each port for availability, and upon finding one, it computes an HTTP inspection port
// by adding 1000 to the gRPC port, initializes a new node (excluding the chosen port from its peers),
// and concurrently launches both the node's gRPC and HTTP inspection services.
func autoDetectServer() {
	ports := []string{"5001", "5002", "5003", "5004", "5005"}

	// Check each port in range
	for i, port := range ports {
		// Check if this port is not in use
		listener, err := net.Listen("tcp", ":"+port)
		if err != nil {
			// If port is already in use, continue to next port
			fmt.Printf("Port %s is in use, checking next port...\n", port)
			continue
		}

		// Port is available, close listener
		if err := listener.Close(); err != nil {
			log.Fatalf("Closing listener %v failed: %v\n", i, err)
		}

		// Calculate port for HTTP inspect
		grpcPort, err := strconv.Atoi(port)
		if err != nil {
			log.Fatalf("Invalid port %s", port)
		}
		inspectPort := strconv.Itoa(grpcPort + 1000)

		// Create a new node
		nodeName := fmt.Sprintf("Node%d", i+1)
		fmt.Printf("[%s] Starting process on port %s...\n", nodeName, port)
		// Construct peers list with localhost prefix for local execution
		peers := []string{}
		for j, p := range ports {
			if i != j {
				peers = append(peers, "localhost:"+p)
			}
		}
		newNode := server.NewNode(nodeName, ports[i], peers)

		// Start gRPC server
		go func() {
			newNode.Start()
		}()

		// Start HTTP server
		go func() {
			newNode.StartHTTP(inspectPort)
		}()

		// Wait a moment for all nodes to start up, then initialize the Raft node
		go func() {
			// Give nodes time to start their gRPC servers
			time.Sleep(3 * time.Second)
			log.Printf("[%s] All nodes should be up now, initializing Raft node", nodeName)
			newNode.InitializeRaftNode()
		}()

		// Stop looking for more ports
		select {}
	}

	// If no empty port found
	log.Fatal("No available port found in range 5001-5005. Ensure these ports are not in use.")
}

// main is the entry point for the Raft-based gRPC application.
// It parses command-line flags to determine how to run the application.
func main() {
	// Add client flag
	clientFlag := flag.Bool("client", false, "Run client to send request")
	leaderAddress := flag.String("leader", "5001", "Leader's address (host:port or just port)")
	term := flag.Int("term", 1, "Leader's term")

	// Add server flags for Docker deployment
	nodeID := flag.String("node_id", "", "Node identifier")
	port := flag.String("port", "", "GRPC port to listen on")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses (host:port)")

	flag.Parse()

	// Check environment variables as alternative to flags
	if *nodeID == "" {
		*nodeID = os.Getenv("NODE_ID")
	}
	if *port == "" {
		*port = os.Getenv("PORT")
	}
	if *peersStr == "" {
		*peersStr = os.Getenv("PEERS")
	}

	// If client flag is used, run client
	if *clientFlag {
		runClient(*leaderAddress, int32(*term))
		return
	}

	// If node_id, port and peers are specified, run as Docker node
	if *nodeID != "" && *port != "" && *peersStr != "" {
		peers := strings.Split(*peersStr, ",")
		runServer(*nodeID, *port, peers)
		return
	}

	// Otherwise, run in auto-detect mode (for local development)
	autoDetectServer()
}
