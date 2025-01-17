package utils

import (
	"context"
	"log"
	"math/rand"
	raftpb "raft-on-the-go/proto"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// Cached connections to peers
var connCache = make(map[string]*grpc.ClientConn)
var connMutex sync.Mutex

type RaftNode struct {
	raftpb.UnimplementedRaftServer
	mu            sync.Mutex
	currentTerm   int
	votedFor      string
	log           []string
	commitIndex   int
	state         string
	peers         []string
	id            string
	votesReceived int
}

type VoteRequest struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type VoteResponse struct {
	Term        int
	VoteGranted bool
}

type AppendRequest struct {
	Term         int
	LeaderId     string
	Entries      []string
	LeaderCommit int
}

type AppendResponse struct {
	Term    int
	Success bool
}

func NewRaftNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		id:            id,
		peers:         peers,
		state:         "follower",
		log:           []string{},
		currentTerm:   0,
		votedFor:      "",
		commitIndex:   0,
		votesReceived: 0,
	}
}

func (rn *RaftNode) StartElectionTimer() {
	for {
		// Randomize election timeout between 1500ms - 3000ms
		timeout := time.Duration(1500+rand.Intn(1500)) * time.Millisecond
		timer := time.NewTimer(timeout)

		select {
		case <-timer.C:
			rn.mu.Lock()
			if rn.state != "leader" {
				log.Printf("%s starting election for term %d", rn.id, rn.currentTerm+1)
				rn.startElection()
			}
			rn.mu.Unlock()
		}
	}
}

func (rn *RaftNode) startHeartbeat() {
	ticker := time.NewTicker(500 * time.Millisecond) // Send heartbeat every 500ms
	defer ticker.Stop()

	for range ticker.C {
		rn.mu.Lock()
		if rn.state != "leader" {
			rn.mu.Unlock()
			return // Stop heartbeats if not leader
		}
		rn.mu.Unlock()

		rn.sendHeartbeats()
		// for _, peer := range rn.peers {
		// 	go rn.sendHeartbeat(peer)
		// }
	}
}

func (rn *RaftNode) sendHeartbeats() {
	var wg sync.WaitGroup

	for _, peer := range rn.peers {
		if peer == rn.id {
			continue // Skip self
		}

		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			rn.sendHeartbeatToPeer(peer)
		}(peer)
	}

	wg.Wait()
}

func (rn *RaftNode) sendHeartbeatToPeer(peer string) {
	// Get or create a cached connection
	conn := rn.getOrCreateConnection(peer)
	if conn == nil {
		return
	}

	client := raftpb.NewRaftClient(conn)

	// Set a timeout for the heartbeat RPC
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := client.Heartbeat(ctx, &raftpb.HeartbeatRequest{
		Term:     int32(rn.currentTerm),
		LeaderId: rn.id,
	})
	if err != nil {
		log.Printf("%s failed to send heartbeat to %s: %v", rn.id, peer, err)
	}
}

func (rn *RaftNode) getOrCreateConnection(peer string) *grpc.ClientConn {
	connMutex.Lock()
	defer connMutex.Unlock()

	// Reuse existing connection if available
	if conn, ok := connCache[peer]; ok {
		return conn
	}

	// Create a new connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "localhost:"+peer,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(2*time.Second),
	)
	if err != nil {
		log.Printf("%s failed to connect to %s: %v", rn.id, peer, err)
		return nil
	}

	connCache[peer] = conn
	return conn
}

func (rn *RaftNode) ResetElectionTimer() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
}

func (rn *RaftNode) ReceiveHeartbeat(req *raftpb.HeartbeatRequest) (*raftpb.HeartbeatResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if req.Term < int32(rn.currentTerm) {
		return &raftpb.HeartbeatResponse{Success: false}, nil
	}
	if req.Term >= int32(rn.currentTerm) {
		rn.currentTerm = int(req.Term)
		rn.votedFor = req.LeaderId
		// rn.ResetElectionTimer()
	}
	return &raftpb.HeartbeatResponse{Success: true}, nil
}

func (rn *RaftNode) startElection() {
	rn.state = "candidate"
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.votesReceived = 1
	log.Printf("%s became candidate for term %d", rn.id, rn.currentTerm)

	for _, peer := range rn.peers {
		go func(peer string) {
			conn, err := grpc.Dial("localhost:"+peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("%s failed to connect to %s: %v", rn.id, peer, err)
				return
			}
			defer conn.Close()

			client := raftpb.NewRaftClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			req := &raftpb.VoteRequest{
				Term:         int32(rn.currentTerm),
				CandidateId:  rn.id,
				LastLogIndex: int32(len(rn.log) - 1),
				LastLogTerm:  0, // Assuming simplified log, update if using full logs
			}

			res, err := client.RequestVote(ctx, req)
			if err != nil {
				log.Printf("%s failed to request vote from %s: %v", rn.id, peer, err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if res.VoteGranted {
				rn.votesReceived++
				log.Printf("%s received vote from %s", rn.id, peer)
			} else if int(res.Term) > rn.currentTerm {
				rn.currentTerm = int(res.Term)
				rn.state = "follower"
				rn.votedFor = ""
			}

			if rn.votesReceived > len(rn.peers)/2 && rn.state == "candidate" {
				rn.state = "leader"
				log.Printf("%s became leader for term %d", rn.id, rn.currentTerm)
				go rn.startHeartbeat() // Start sending heartbeats
			}
		}(peer)
	}
}

func (rn *RaftNode) HandleRequestVote(req *VoteRequest) *VoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.state = "follower"
	}

	voteGranted := false
	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) && req.Term >= rn.currentTerm {
		rn.votedFor = req.CandidateId
		voteGranted = true
		log.Printf("%s voted for %s in term %d", rn.id, req.CandidateId, req.Term)
	}

	return &VoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: voteGranted,
	}
}

func (rn *RaftNode) HandleAppendEntries(req *AppendRequest) *AppendResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < rn.currentTerm {
		return &AppendResponse{
			Term:    rn.currentTerm,
			Success: false,
		}
	}

	// Reset to follower if leader term is higher or the same
	rn.state = "follower"
	rn.currentTerm = req.Term
	rn.ResetElectionTimer()

	if len(req.Entries) > 0 {
		rn.log = append(rn.log, req.Entries...)
		log.Printf("%s received AppendEntries from %s", rn.id, req.LeaderId)
	} else {
		log.Printf("%s received heartbeat from leader %s", rn.id, req.LeaderId)
	}

	return &AppendResponse{
		Term:    rn.currentTerm,
		Success: true,
	}
}
