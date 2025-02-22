package utils

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
	pb "raft-on-the-go/proto"
	"google.golang.org/grpc"
)

const (
	HeartbeatInterval  = 1000 * time.Millisecond
	MinElectionTimeout = 3000 * time.Millisecond
	MaxElectionTimeout = 5000 * time.Millisecond
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type RaftNode struct {
	pb.UnimplementedRaftServer
	mu             sync.Mutex
	id             string
	peers          []string
	currentTerm    int
	votedFor       string
	State          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	grpcServer     *grpc.Server
	KVStore        *KVStore
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
		State:         State(Follower),
		grpcServer:    grpc.NewServer(),
		KVStore:       NewKVStore(),
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

	rn.State = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id // Vote its self
	votes := 1
	log.Printf("[%s] Term %d: Initialized, waiting for election timeout...", rn.id, rn.currentTerm)

	// send RequestVote for other node
	for _, peer := range rn.peers {
		go func(peer string) {
			conn, err := grpc.Dial("localhost:"+peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] Failed to connect to %s: %v", rn.id, peer, err)
				return
			}
			defer conn.Close()

			client := pb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()

			req := &pb.VoteRequest{CandidateId: rn.id, Term: int32(rn.currentTerm)}
			resp, err := client.RequestVote(ctx, req)
			// log.Printf("[%s] Received response from %s: %v", rn.id, peer, resp)
			if err == nil && resp.VoteGranted {
				rn.mu.Lock()
				votes++

				if rn.State == Candidate && votes > len(rn.peers)/2 {
					rn.State = Leader
					rn.resetHeartbeatTimer()
					log.Printf("[%s] Term %d: Received majority votes, becoming Leader", rn.id, rn.currentTerm)
				}
				rn.mu.Unlock()
			}
		}(peer)
	}
}

func (rn *RaftNode) sendHeartbeats() {
	if rn.State != Leader {
		return
	}

	for _, peer := range rn.peers {
		go func(peer string) {
			conn, err := grpc.Dial("localhost:"+peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] Term %d: Failed to send heartbeat to %s: %v", rn.id, rn.currentTerm, peer, err)
			}
			defer conn.Close()

			client := pb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()

			_, err = client.Heartbeat(ctx, &pb.HeartbeatRequest{LeaderId: rn.id, Term: int32(rn.currentTerm)})
			if err != nil {
				log.Printf("[%s] Term %d: Tried and failed to send heartbeat to %s: %v", rn.id, rn.currentTerm, peer, err)
			} else {
				// log.Printf("[%s] Term %d: Sent heartbeat to %s", rn.id, rn.currentTerm, peer)
			}
		}(peer)
	}

	rn.resetHeartbeatTimer()
}

func (rn *RaftNode) ReceiveHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < int32(rn.currentTerm) {
		return &pb.HeartbeatResponse{Success: false}, nil
	}

	if req.Term > int32(rn.currentTerm) {
		rn.currentTerm = int(req.Term)
		rn.State = Follower
		rn.votedFor = ""
	}
	rn.resetElectionTimer() // Reset election timer upon receiving valid heartbeat
	return &pb.HeartbeatResponse{Success: true}, nil
}

func (rn *RaftNode) HandleRequestVote(req *VoteRequest) *VoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if req.Term >= rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.State = Follower
	}

	voteGranted := false
	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) && req.Term >= rn.currentTerm {
		rn.votedFor = req.CandidateId
		voteGranted = true
		rn.resetElectionTimer()
		log.Printf("[%s] Term %d: voted for %s", rn.id, req.Term, req.CandidateId)
	}

	return &VoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: voteGranted,
	}
}

func (rn *RaftNode) HandleAppendEntries(req *AppendRequest) *AppendResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Nếu term của leader thấp hơn, từ chối yêu cầu
	if req.Term < rn.currentTerm {
		return &AppendResponse{
			Term:    rn.currentTerm,
			Success: false,
		}
	}

	// Nếu term của leader cao hơn, cập nhật term và ghi lại log mới
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.State = Follower
		rn.votedFor = ""
	}

	// // Reset to follower if leader term is higher or the same
	// rn.state = Follower
	// rn.currentTerm = req.Term
	// rn.resetElectionTimer()


	// if len(req.Entries) > 0 {
	// 	rn.log = append(rn.log, req.Entries...)
	// 	log.Printf("[%s] Term %d: Received AppendEntries from %s", rn.id, rn.currentTerm, req.LeaderId)
	// } else {
	// 	log.Printf("[%s] Term %d: Received HeartBeat from leader %s", rn.id, rn.currentTerm, req.LeaderId)
	// }

	// Thêm các entry vào log
	rn.log = append(rn.log, req.Entries...)
	rn.KVStore.SyncData(req.Entries) // Đồng bộ dữ liệu vào KVStore
	
	// log.Printf("Node %s Appending Entry: %v", rn.id, req.Entries)
	// log.Printf("[%s] Term %d: Received AppendEntries from Leader %s, appending log: %v", rn.id, rn.currentTerm, req.LeaderId, req.Entries)
	log.Printf("[%s] Term %d: Received AppendEntries from Leader %s", rn.id, rn.currentTerm, req.LeaderId)
	log.Printf("\t\tLog entries: %v", req.Entries)



	return &AppendResponse{
		Term:    rn.currentTerm,
		Success: true,
	}
}


// GetCurrentTerm trả về currentTerm của RaftNode
func (rn *RaftNode) GetCurrentTerm() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm
}

// GetLog trả về bản sao log của RaftNode
func (rn *RaftNode) GetLog() []string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	// Trả về một bản sao để tránh race condition
	logCopy := make([]string, len(rn.log))
	copy(logCopy, rn.log)
	return logCopy
}