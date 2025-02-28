package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	pb "raft-on-the-go/proto"
	raftpb "raft-on-the-go/proto"
	"raft-on-the-go/utils"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type Node struct {
	pb.RaftServer

	id         string
	port       string
	peers      []string
	RaftNode   *utils.RaftNode
	grpcServer *grpc.Server
}

func (n *Node) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	log.Printf("[%s] Term %d: Request vote", req.CandidateId, req.Term)
	response := n.RaftNode.HandleRequestVote(&utils.VoteRequest{
		Term:         int(req.Term),
		CandidateId:  req.CandidateId,
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	})
	return &pb.VoteResponse{
		Term:        int32(response.Term),
		VoteGranted: response.VoteGranted,
	}, nil
}

func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	// Validate the request term first - make sure it's the same as our current term if we're the leader
	// This prevents term inflation from incorrect client requests
	if n.RaftNode.GetState() == utils.Leader && int(req.Term) != n.RaftNode.GetCurrentTerm() {
		log.Printf("[%s] Leader received AppendEntries with incorrect term %d (current: %d), correcting",
			n.id, req.Term, n.RaftNode.GetCurrentTerm())
		req.Term = int32(n.RaftNode.GetCurrentTerm())
	}

	// Update KVStore with entries if they come from a recognized leader
	if req.LeaderId != "" {
		n.RaftNode.KVStore.SyncData(req.Entries)
	}

	// Handle the append entries request through the Raft implementation
	response := n.RaftNode.HandleAppendEntries(&utils.AppendRequest{
		Term:         int(req.Term),
		LeaderId:     req.LeaderId,
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      req.Entries,
		LeaderCommit: int(req.LeaderCommit),
	})

	// If we're a leader, replicate these entries to followers
	if n.RaftNode.GetState() == utils.Leader {
		// If the request didn't include a leader ID, set it to our ID
		if req.LeaderId == "" {
			req.LeaderId = n.id
		}

		// Ensure the request has our correct term
		req.Term = int32(n.RaftNode.GetCurrentTerm())

		// Replicate to followers
		go n.replicateEntries(req)
	}

	return &pb.AppendResponse{
		Term:      int32(response.Term),
		Success:   response.Success,
		NextIndex: int32(response.NextIndex),
	}, nil
}

// Replicate entries to all followers
func (n *Node) replicateEntries(req *pb.AppendRequest) {
	// Exit early if we're not the leader anymore
	if n.RaftNode.GetState() != utils.Leader {
		log.Printf("[%s] No longer leader, canceling replication", n.id)
		return
	}

	// We count ourselves as successful since we've already processed the entry
	successCount := 1
	responseCh := make(chan bool, len(n.peers))

	// Get latest log info
	prevLogIndex := 0
	prevLogTerm := 0
	if len(n.RaftNode.GetLog()) > 0 {
		prevLogIndex = len(n.RaftNode.GetLog())
		prevLogTerm = int(n.RaftNode.GetLog()[prevLogIndex-1].Term)
	}

	// Make sure we're using current information
	req.Term = int32(n.RaftNode.GetCurrentTerm())
	req.LeaderCommit = int32(n.RaftNode.GetCommitIndex())
	req.PrevLogIndex = int32(prevLogIndex)
	req.PrevLogTerm = int32(prevLogTerm)

	// Use a wait group to handle all the responses
	var wg sync.WaitGroup

	// Send the append entries request to each follower
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			// Try to connect to the follower
			conn, err := grpc.Dial(peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] Failed to connect to follower %s: %v", n.id, peer, err)
				responseCh <- false
				return
			}
			defer conn.Close()

			// Create a client and send the request
			follower := pb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()

			// Keep a copy of our current term for checking later
			currentTerm := n.RaftNode.GetCurrentTerm()

			resp, err := follower.AppendEntries(ctx, req)
			if err != nil {
				log.Printf("[%s] Error replicating to follower %s: %v", n.id, peer, err)
				responseCh <- false
				return
			}

			log.Printf("[%s] Follower %s responded: Term=%d, Success=%v, NextIndex=%d",
				n.id, peer, resp.Term, resp.Success, resp.NextIndex)

			// If follower has a higher term, we need to step down
			if resp.Term > int32(currentTerm) {
				log.Printf("[%s] Term %d: Follower %s has higher term %d, stepping down",
					n.id, currentTerm, peer, resp.Term)

				// Use the StepDownToFollower method instead of sending ourselves a message
				n.RaftNode.StepDownToFollower(int(resp.Term))

				// Signal that we failed the replication and return early
				responseCh <- false
				return
			}

			// Otherwise, report success or failure based on follower's response
			responseCh <- resp.Success
		}(peer)
	}

	// Wait for all the replication attempts to complete
	wg.Wait()
	close(responseCh)

	// Count successful replications
	for success := range responseCh {
		if success {
			successCount++
		}
	}

	// Final check that we're still leader before updating commit index
	if n.RaftNode.GetState() != utils.Leader {
		log.Printf("[%s] No longer leader after replication, skipping commit", n.id)
		return
	}

	// If a majority of nodes (including us) succeeded, update the commit index
	if successCount > len(n.peers)/2 {
		newCommitIndex := len(n.RaftNode.GetLog())
		n.RaftNode.UpdateCommitIndex(newCommitIndex)
	} else {
		log.Printf("[%s] Failed to achieve quorum (%d/%d successful), not committing",
			n.id, successCount, len(n.peers)+1)
	}
}

// Heartbeat RPC Implementation
func (n *Node) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	log.Printf("[%s] Term %d: Received heartbeat from Leader %s ", n.id, req.Term, req.LeaderId)

	// Process the heartbeat through the Raft node
	response, err := n.RaftNode.ReceiveHeartbeat(req)

	if err == nil && response.NeedsSync {
		log.Printf("[%s] Term %d: Node needs log synchronization with Leader %s", n.id, req.Term, req.LeaderId)
	}

	return response, err
}

// NewNode creates a new Node instance with the specified identifier, port, and peer addresses.
// It also initializes the underlying Raft consensus mechanism by creating a new RaftNode with the provided id and peers.
func NewNode(id, port string, peers []string) *Node {
	return &Node{
		id:       id,
		port:     port,
		peers:    peers,
		RaftNode: utils.NewRaftNode(id, peers),
	}
}

// Start initializes and runs the gRPC server
func (n *Node) Start() {
	lis, err := net.Listen("tcp", ":"+n.port)
	if err != nil {
		log.Fatalf("%s failed to listen: %v", n.id, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, n)
	n.grpcServer = grpcServer

	fmt.Printf("[%s] Running on port %s\n", n.id, n.port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("%s failed to serve: %v", n.id, err)
	}
}

// inspectHandler returns node information in JSON format
func (n *Node) inspectHandler(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"node_id":      n.id,
		"current_term": n.RaftNode.GetCurrentTerm(),
		"state":        n.RaftNode.GetState().StateString(),
		"log_entries":  n.RaftNode.GetLog(),
		"commit_index": n.RaftNode.GetCommitIndex(),
		"last_applied": n.RaftNode.GetLastApply(),
		"kv_store":     n.RaftNode.KVStore.GetStore(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// shutdownHandler gracefully shuts down the node
func (n *Node) shutdownHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	data := map[string]string{"message": "Unable to shutdown server"}
	w.Header().Set("Content-Type", "application/json")

	log.Printf("Initiating graceful shutdown for %s %s...", n.RaftNode.GetState().StateString(), n.id)
	timer := time.AfterFunc(10*time.Second, func() {
		log.Println("Server could not stop gracefully in time. Performing force shutdown.")
		n.grpcServer.Stop()
		data["message"] = "Server stopped forcefully"
	})
	defer timer.Stop()

	n.RaftNode.Shutdown()
	n.grpcServer.GracefulStop()
	log.Println("Server stopped gracefully.")
	data["message"] = "Server stopped gracefully"

	json.NewEncoder(w).Encode(data)
}

// appendEntryHandler handles HTTP requests to append an entry
func (n *Node) appendEntryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Check if this node is a leader before doing anything else
	if n.RaftNode.GetState() != utils.Leader {
		log.Printf("[%s] Rejecting append request - not a leader", n.id)
		http.Error(w, "Node is not a leader", http.StatusForbidden)
		return
	}

	// Parse the request body
	var req pb.AppendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// IMPORTANT: Always use the leader's current term, ignoring any term in the client request
	currentTerm := n.RaftNode.GetCurrentTerm()
	
	// Log the request for debugging
	log.Printf("[%s] HTTP append request received with %d entries, using leader term %d", 
		n.id, len(req.Entries), currentTerm)

	// Get log info for prevLogIndex and prevLogTerm
	prevLogIndex := 0
	prevLogTerm := 0
	if len(n.RaftNode.GetLog()) > 0 {
		prevLogIndex = len(n.RaftNode.GetLog())
		prevLogTerm = int(n.RaftNode.GetLog()[prevLogIndex-1].Term)
	}

	// Create a clean AppendRequest with current leader state
	appendReq := &raftpb.AppendRequest{
		Term:         int32(currentTerm),        // Always use leader's current term
		LeaderId:     n.id,                      // Set correct leader ID
		PrevLogIndex: int32(prevLogIndex),
		PrevLogTerm:  int32(prevLogTerm),
		Entries:      req.Entries,               // Keep client's entries
		LeaderCommit: int32(n.RaftNode.GetCommitIndex()),
	}

	// Process through normal append entries path
	ctx := context.Background()
	response, _ := n.AppendEntries(ctx, appendReq)

	// Return the response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&pb.AppendResponse{
		Term:      int32(response.Term),
		Success:   response.Success,
		NextIndex: response.NextIndex,
	})
}

// StartHTTP runs an HTTP server on the given port for node inspection and control
func (n *Node) StartHTTP(httpPort string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/inspect", n.inspectHandler)
	mux.HandleFunc("/shutdown", n.shutdownHandler)
	mux.HandleFunc("/append", n.appendEntryHandler)
	log.Printf("[%s] HTTP inspect endpoint running on port %s", n.id, httpPort)
	if err := http.ListenAndServe(":"+httpPort, mux); err != nil {
		log.Fatalf("[%s] HTTP server error: %v", n.id, err)
	}
}