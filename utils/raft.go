package utils

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	raftpb "raft-on-the-go/proto"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const (
	HeartbeatInterval  = 500 * time.Millisecond
	MinElectionTimeout = 1500 * time.Millisecond
	MaxElectionTimeout = 3000 * time.Millisecond
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type RaftNode struct {
	raftpb.UnimplementedRaftServer
	mu             sync.Mutex
	id             string
	peers          []string
	currentTerm    int
	votedFor       string
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	grpcServer     *grpc.Server
	log            []string
	commitIndex    int
	votesReceived  int
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
	rn := &RaftNode{
		id:            id,
		peers:         peers,
		currentTerm:   0,
		votedFor:      "",
		state:         State(Follower),
		grpcServer:    grpc.NewServer(),
		log:           []string{},
		commitIndex:   0,
		votesReceived: 0,
	}
	rn.resetElectionTimer()
	return rn
}

func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	timeout := MinElectionTimeout + time.Duration(rand.Intn(int(MaxElectionTimeout-MinElectionTimeout)))
	rn.electionTimer = time.AfterFunc(timeout, rn.startElection)
}

func (rn *RaftNode) resetHeartbeatTimer() {
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	rn.heartbeatTimer = time.AfterFunc(HeartbeatInterval, rn.sendHeartbeats) // TODO: Check for Leader state
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	votes := 1
	log.Printf("%s starting election for term %d", rn.id, rn.currentTerm)

	for _, peer := range rn.peers {
		go func(peer string) {
			conn, err := grpc.Dial("localhost:"+peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("%s failed to connect to %s: %v", rn.id, peer, err)
				return
			}
			defer conn.Close()

			client := raftpb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()

			req := &raftpb.VoteRequest{CandidateId: rn.id, Term: int32(rn.currentTerm)}
			resp, err := client.RequestVote(ctx, req)
			log.Printf("%s received response from %s: %v", rn.id, peer, resp)
			if err == nil && resp.VoteGranted {
				rn.mu.Lock()
				votes++
				if votes > len(rn.peers) && rn.state == Candidate {
					rn.state = Leader
					rn.resetHeartbeatTimer()
					log.Printf("%s became the leader for term %d", rn.id, rn.currentTerm)
				}
				rn.mu.Unlock()
			}
		}(peer)
	}
}

func (rn *RaftNode) sendHeartbeats() {
	if rn.state != Leader {
		return
	}

	for _, peer := range rn.peers {
		go func(peer string) {
			conn, err := grpc.Dial("localhost:"+peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("%s failed to send heartbeat to %s: %v", rn.id, peer, err)
			}
			defer conn.Close()

			client := raftpb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()

			_, err = client.Heartbeat(ctx, &raftpb.HeartbeatRequest{LeaderId: rn.id, Term: int32(rn.currentTerm)})
			if err != nil {
				log.Printf("%s tried and failed to send heartbeat to %s: %v", rn.id, peer, err)
			}
		}(peer)
	}

	rn.resetHeartbeatTimer()
}

func (rn *RaftNode) ReceiveHeartbeat(req *raftpb.HeartbeatRequest) (*raftpb.HeartbeatResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < int32(rn.currentTerm) {
		return &raftpb.HeartbeatResponse{Success: false}, nil
	}

	if req.Term > int32(rn.currentTerm) {
		rn.currentTerm = int(req.Term)
		rn.state = Follower
		rn.votedFor = ""
	}
	rn.resetElectionTimer() // Reset election timer upon receiving valid heartbeat
	return &raftpb.HeartbeatResponse{Success: true}, nil
}

func (rn *RaftNode) HandleRequestVote(req *VoteRequest) *VoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if req.Term >= rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.state = Follower
	}

	voteGranted := false
	fmt.Printf("%t %t %t\n", rn.votedFor == "", rn.votedFor == req.CandidateId, req.Term >= rn.currentTerm)
	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) && req.Term >= rn.currentTerm {
		rn.votedFor = req.CandidateId
		voteGranted = true
		rn.resetElectionTimer()
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
	rn.state = Follower
	rn.currentTerm = req.Term
	rn.resetElectionTimer()

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
