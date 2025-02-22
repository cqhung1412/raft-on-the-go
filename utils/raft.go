package utils

import (
	"context"
	"log"
	"math/rand"
	pb "raft-on-the-go/proto"
	"strings"
	"sync"
	"time"

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
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	grpcServer     *grpc.Server
	KVStore        *KVStore
	log            []*pb.LogEntry
	commitIndex    int
	lastApplied    int
	votesReceived  int
	shutdownCh     chan struct{}
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
	Entries      []*pb.LogEntry
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
		KVStore:       NewKVStore(),
		log:           []*pb.LogEntry{},
		commitIndex:   0,
		votesReceived: 0,
		shutdownCh:    make(chan struct{}),
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

			if err == nil && resp.VoteGranted {
				rn.mu.Lock()
				votes++

				if rn.state == Candidate && votes > len(rn.peers)/2 {
					rn.state = Leader
					rn.resetHeartbeatTimer()
					log.Printf("[%s] Term %d: Received majority votes, becoming Leader", rn.id, rn.currentTerm)
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
		rn.state = Follower
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
		rn.state = Follower
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

	// Nếu node không phải là Leader, tức là đang ở trạng thái Follower,
	// và nếu LeaderId trong request không khớp với leader đã được công nhận (hoặc rỗng),
	// ta từ chối yêu cầu.
	// Ở ví dụ này, chúng ta giả sử rằng follower chỉ chấp nhận nếu nó tự nhận mình là follower của leader.
	// Nếu nó nhận yêu cầu AppendEntries trong trạng thái Follower từ nguồn không xác định, trả về false.
	if rn.state != Leader {
		// Ở một hệ thống Raft hoàn chỉnh, follower sẽ chấp nhận các AppendEntries từ leader hợp lệ
		// và cập nhật lại leader hiện tại. Tuy nhiên, nếu nhận được request từ một nguồn khác
		// (ví dụ, từ client gửi trực tiếp), LeaderId có thể không hợp lệ.
		// Ở đây, nếu LeaderId rỗng hoặc không khớp (nếu bạn có lưu leader hiện tại), thì từ chối.
		if req.LeaderId == "" /* || req.LeaderId != rn.leaderID (nếu bạn lưu thông tin leader) */ {
			log.Printf("[%s] Term %d: Rejecting AppendEntries from invalid source (LeaderId='%s')", rn.id, rn.currentTerm, req.LeaderId)
			return &AppendResponse{
				Term:    rn.currentTerm,
				Success: false,
			}
		}
	}

	// Nếu term của leader cao hơn, cập nhật term và ghi lại log mới
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.state = Follower
		rn.votedFor = ""
	}

	log.Printf("[%s] Term %d: Received AppendEntries from Leader %s", rn.id, rn.currentTerm, req.LeaderId)

	// Tạo log entry cho mỗi command và append vào log
	for _, cmd := range req.Entries {
		entry := &pb.LogEntry{
			Index:   int32(len(rn.log) + 1), // Có thể sử dụng commitIndex + 1 hoặc len(rn.log)+1
			Term:    int32(rn.currentTerm),
			Command: cmd.Command,
		}
		rn.log = append(rn.log, entry)
		log.Printf("[%s] Term %d: Apply command: %v - at index %d", rn.id, rn.currentTerm, entry.Command, entry.Index)
	}

	// Cập nhật commitIndex nếu LeaderCommit (gửi từ leader) cao hơn commitIndex hiện tại của follower
	if int(req.LeaderCommit) > rn.commitIndex {
		newCommitIndex := int(req.LeaderCommit)
		if newCommitIndex > len(rn.log) {
			newCommitIndex = len(rn.log)
		}
		rn.updateCommitIndexInternal(newCommitIndex)
	}

	return &AppendResponse{
		Term:    rn.currentTerm,
		Success: true,
	}
}

// UpdateCommitIndex là phiên bản exported, tự lock mutex
func (rn *RaftNode) UpdateCommitIndex(newCommitIndex int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.updateCommitIndexInternal(newCommitIndex)
}

// updateCommitIndexInternal cập nhật commitIndex và apply các entry mới.
// Lưu ý: Hàm này giả định rằng rn.mu đã được lock.
func (rn *RaftNode) updateCommitIndexInternal(newCommitIndex int) {
	if newCommitIndex > rn.commitIndex {
		rn.commitIndex = newCommitIndex
		log.Printf("[%s] Term %d: Updated commitIndex to %d", rn.id, rn.currentTerm, rn.commitIndex)
		// Áp dụng các entry từ lastApplied+1 đến commitIndex
		for rn.lastApplied < rn.commitIndex {
			rn.lastApplied++
			entry := rn.log[rn.lastApplied-1] // Vì rn.lastApplied tính theo 1-indexed
			parts := strings.SplitN(entry.Command, "=", 2)
			if len(parts) == 2 {
				rn.KVStore.Set(parts[0], parts[1])
			}
			log.Printf("[%s] Term %d: Applied entry at index %d, command: %s", rn.id, rn.currentTerm, entry.Index, entry.Command)
		}
	}
}

// GetCurrentTerm trả về currentTerm của RaftNode
func (rn *RaftNode) GetCurrentTerm() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm
}

// GetLog trả về bản sao log của RaftNode
func (rn *RaftNode) GetLog() []*pb.LogEntry {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	// Trả về một bản sao để tránh race condition
	logCopy := make([]*pb.LogEntry, len(rn.log))
	copy(logCopy, rn.log)
	return logCopy
}

// GetState trả về trạng thái hiện tại
func (rn *RaftNode) GetState() State {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state
}

// GetCommitIndex trả về commitIndex của RaftNode
func (rn *RaftNode) GetCommitIndex() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.commitIndex
}

// GetLastApply trả về lastApply của RaftNode
func (rn *RaftNode) GetLastApply() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.lastApplied
}

// chuyen state tu int -> State string
func (s State) StateString() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (rn *RaftNode) Shutdown() {
	close(rn.shutdownCh)
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	rn.grpcServer.GracefulStop()
	log.Printf("[%s] Raft node shutdown complete", rn.id)
}
