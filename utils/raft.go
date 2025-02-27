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

	// Leader state - only used when node is leader
	nextIndex  map[string]int // For each peer, index of the next log entry to send
	matchIndex map[string]int // For each peer, index of highest log entry known to be replicated
	leaderID   string         // ID of the current leader (empty if unknown)
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
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*pb.LogEntry
	LeaderCommit int
}

type AppendResponse struct {
	Term      int
	Success   bool
	NextIndex int
}

// NewRaftNode creates and returns a new RaftNode with the specified unique node identifier and list of peer addresses.
// It initializes the node in the Follower state with a current term of 0, sets up a gRPC server and a key-value store, and prepares an empty log.
// The function also resets the election timer and initializes the commit index, vote count, and shutdown channel for graceful termination.
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
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
		leaderID:      "",
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

	// Get the last log info for the vote request
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rn.log) > 0 {
		lastLogIndex = len(rn.log)
		lastLogTerm = int(rn.log[lastLogIndex-1].Term)
	}

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

			req := &pb.VoteRequest{
				CandidateId:  rn.id,
				Term:         int32(rn.currentTerm),
				LastLogIndex: int32(lastLogIndex),
				LastLogTerm:  int32(lastLogTerm),
			}
			resp, err := client.RequestVote(ctx, req)

			if err == nil && resp.VoteGranted {
				rn.mu.Lock()
				votes++

				if rn.state == Candidate && votes > len(rn.peers)/2 {
					rn.state = Leader

					// Initialize leader state when becoming leader
					for _, p := range rn.peers {
						rn.nextIndex[p] = len(rn.log) + 1
						rn.matchIndex[p] = 0
					}

					rn.resetHeartbeatTimer()
					log.Printf("[%s] Term %d: Received majority votes, becoming Leader", rn.id, rn.currentTerm)
				}
				rn.mu.Unlock()
			}
		}(peer)
	}
}

func (rn *RaftNode) sendHeartbeats() {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return
	}

	// Leader state data for the heartbeat
	currentTerm := rn.currentTerm
	commitIndex := rn.commitIndex
	lastLogIndex := len(rn.log)
	rn.mu.Unlock()

	for _, peer := range rn.peers {
		go func(peer string) {
			conn, err := grpc.Dial("localhost:"+peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] Term %d: Failed to connect to %s for heartbeat: %v", rn.id, currentTerm, peer, err)
				return
			}
			defer conn.Close()

			client := pb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()

			req := &pb.HeartbeatRequest{
				LeaderId:     rn.id,
				Term:         int32(currentTerm),
				LeaderCommit: int32(commitIndex),
				LastLogIndex: int32(lastLogIndex),
			}

			resp, err := client.Heartbeat(ctx, req)
			if err != nil {
				log.Printf("[%s] Term %d: Tried and failed to send heartbeat to %s: %v", rn.id, currentTerm, peer, err)
				return
			}

			// If follower's log is behind, send AppendEntries to sync logs
			if resp.NeedsSync {
				rn.mu.Lock()
				// Check if we're still leader
				if rn.state != Leader {
					rn.mu.Unlock()
					return
				}

				nextIdx := rn.nextIndex[peer]
				// If follower's log is behind, adjust nextIndex
				if int(resp.LastLogIndex) < len(rn.log) {
					// Send log entries starting from nextIdx
					prevLogIndex := nextIdx - 1
					prevLogTerm := 0

					if prevLogIndex > 0 && prevLogIndex <= len(rn.log) {
						prevLogTerm = int(rn.log[prevLogIndex-1].Term)
					}

					// Prepare log entries to send
					entries := []*pb.LogEntry{}
					if nextIdx <= len(rn.log) {
						entries = rn.log[nextIdx-1:]
					}

					appendReq := &pb.AppendRequest{
						Term:         int32(rn.currentTerm),
						LeaderId:     rn.id,
						PrevLogIndex: int32(prevLogIndex),
						PrevLogTerm:  int32(prevLogTerm),
						Entries:      entries,
						LeaderCommit: int32(rn.commitIndex),
					}

					rn.mu.Unlock()

					// Send AppendEntries RPC to sync logs
					go rn.sendAppendEntriesToPeer(peer, appendReq)
				} else {
					rn.mu.Unlock()
				}
			}
		}(peer)
	}

	rn.resetHeartbeatTimer()
}

// Helper method to send AppendEntries to a specific peer for log replication
func (rn *RaftNode) sendAppendEntriesToPeer(peer string, req *pb.AppendRequest) {
	conn, err := grpc.Dial("localhost:"+peer, grpc.WithInsecure())
	if err != nil {
		log.Printf("[%s] Term %d: Failed to connect to %s for log replication: %v", rn.id, req.Term, peer, err)
		return
	}
	defer conn.Close()

	client := pb.NewRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		log.Printf("[%s] Term %d: Failed to replicate logs to %s: %v", rn.id, req.Term, peer, err)
		return
	}

	// Handle the response
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// If the follower rejected our request because its term is higher
	if !resp.Success && resp.Term > int32(rn.currentTerm) {
		rn.currentTerm = int(resp.Term)
		rn.state = Follower
		rn.votedFor = ""
		rn.resetElectionTimer()
		return
	}

	// Update nextIndex and matchIndex for this peer if successful
	if resp.Success {
		rn.nextIndex[peer] = int(resp.NextIndex)
		rn.matchIndex[peer] = rn.nextIndex[peer] - 1

		// Update commitIndex if needed
		rn.updateCommitIndexBasedOnReplications()
	} else {
		// If the AppendEntries failed, decrement nextIndex and try again
		if rn.nextIndex[peer] > 1 {
			rn.nextIndex[peer]--
		}
	}
}

// Helper method to determine if a log entry is committed based on replication to followers
func (rn *RaftNode) updateCommitIndexBasedOnReplications() {
	// Sort the match indices to find the median (majority)
	matchIndices := make([]int, 0, len(rn.peers))
	for _, idx := range rn.matchIndex {
		matchIndices = append(matchIndices, idx)
	}

	// Add leader's match index (which is essentially the log length)
	matchIndices = append(matchIndices, len(rn.log))

	// Simple sort to find the median - in production you'd optimize this
	for i := 0; i < len(matchIndices); i++ {
		for j := i + 1; j < len(matchIndices); j++ {
			if matchIndices[i] > matchIndices[j] {
				matchIndices[i], matchIndices[j] = matchIndices[j], matchIndices[i]
			}
		}
	}

	// The middle element is the majority-replicated index
	majorityIndex := matchIndices[len(matchIndices)/2]

	// If the majority index is greater than our commit index, update it
	if majorityIndex > rn.commitIndex &&
		(len(rn.log) >= majorityIndex) &&
		(int(rn.log[majorityIndex-1].Term) == rn.currentTerm) {
		rn.updateCommitIndexInternal(majorityIndex)
	}
}

func (rn *RaftNode) ReceiveHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	response := &pb.HeartbeatResponse{
		Success:      false,
		Term:         int32(rn.currentTerm),
		LastLogIndex: int32(len(rn.log)),
		NeedsSync:    false,
	}

	// If the leader's term is less than ours, reject heartbeat
	if req.Term < int32(rn.currentTerm) {
		return response, nil
	}

	// If the leader's term is greater than ours, update our term
	if req.Term > int32(rn.currentTerm) {
		rn.currentTerm = int(req.Term)
		rn.state = Follower
		rn.votedFor = ""
	}

	// Store the leader ID
	rn.leaderID = req.LeaderId

	// Reset election timer upon receiving valid heartbeat
	rn.resetElectionTimer()

	// Check if our log is behind the leader's
	response.Success = true

	// If leader has more entries than us, or if our commit index is behind
	// leader's commit index, we need to sync
	if req.LastLogIndex > int32(len(rn.log)) ||
		req.LeaderCommit > int32(rn.commitIndex) {
		response.NeedsSync = true
	}

	// Update commit index if needed
	if req.LeaderCommit > int32(rn.commitIndex) {
		newCommitIdx := int(req.LeaderCommit)
		if newCommitIdx > len(rn.log) {
			newCommitIdx = len(rn.log)
		}
		rn.updateCommitIndexInternal(newCommitIdx)
	}

	return response, nil
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

	response := &AppendResponse{
		Term:      rn.currentTerm,
		Success:   false,
		NextIndex: len(rn.log) + 1,
	}

	// If leader's term is less than ours, reject the request
	if req.Term < rn.currentTerm {
		log.Printf("[%s] Term %d: Rejecting AppendEntries from leader %s with lesser term %d",
			rn.id, rn.currentTerm, req.LeaderId, req.Term)
		return response
	}

	// If leader's term is greater than ours, update our term
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.state = Follower
		rn.votedFor = ""
		response.Term = rn.currentTerm
	}

	// If not the leader and the leader ID is invalid, reject
	if rn.state != Leader {
		if req.LeaderId == "" || (rn.leaderID != "" && req.LeaderId != rn.leaderID) {
			log.Printf("[%s] Term %d: Rejecting AppendEntries from invalid source (LeaderId='%s')",
				rn.id, rn.currentTerm, req.LeaderId)
			return response
		}
	}

	// Store the leader ID
	rn.leaderID = req.LeaderId
	rn.resetElectionTimer()

	// Log consistency check: if prevLogIndex is specified, ensure we have that entry
	// with the correct term
	prevLogIndex := int(req.PrevLogIndex)
	if prevLogIndex > 0 {
		// If our log doesn't reach prevLogIndex or term doesn't match, reject
		if prevLogIndex > len(rn.log) ||
			(prevLogIndex > 0 && int(rn.log[prevLogIndex-1].Term) != int(req.PrevLogTerm)) {
			log.Printf("[%s] Term %d: Log inconsistency detected. PrevLogIndex: %d, Log length: %d",
				rn.id, rn.currentTerm, prevLogIndex, len(rn.log))

			// Tell the leader what index to start sending from
			if prevLogIndex > len(rn.log) {
				response.NextIndex = len(rn.log) + 1
			} else {
				// Find the last index for the conflicting term
				conflictTerm := rn.log[prevLogIndex-1].Term
				for i := prevLogIndex - 2; i >= 0; i-- {
					if rn.log[i].Term != conflictTerm {
						response.NextIndex = i + 2
						break
					}
				}
				if response.NextIndex == prevLogIndex {
					response.NextIndex = 1 // Start from beginning
				}
			}
			return response
		}
	}

	log.Printf("[%s] Term %d: Processing AppendEntries from Leader %s", rn.id, rn.currentTerm, req.LeaderId)

	// If we made it here, log consistency check passed

	// If there are entries to append
	if len(req.Entries) > 0 {
		// Handle entries: append new entries, removing any conflicting entries
		logIdx := prevLogIndex

		// Process each entry
		for _, entry := range req.Entries {
			logIdx++

			// If we're replacing an existing entry
			if logIdx <= len(rn.log) {
				// If existing entry conflicts with new one, delete it and all after it
				if rn.log[logIdx-1].Term != entry.Term {
					rn.log = rn.log[:logIdx-1]
					// Add the new entry
					rn.log = append(rn.log, entry)
					log.Printf("[%s] Term %d: Replaced conflicting entry at index %d",
						rn.id, rn.currentTerm, logIdx)
				}
				// Otherwise existing entry matches, keep it
			} else {
				// This is a new entry beyond our log, append it
				rn.log = append(rn.log, entry)
				log.Printf("[%s] Term %d: Appended new entry at index %d: %s",
					rn.id, rn.currentTerm, logIdx, entry.Command)
			}
		}
	}

	// Update commit index if leader's commit index is higher
	if int(req.LeaderCommit) > rn.commitIndex {
		newCommitIndex := int(req.LeaderCommit)
		if newCommitIndex > len(rn.log) {
			newCommitIndex = len(rn.log)
		}
		rn.updateCommitIndexInternal(newCommitIndex)
	}

	// Update the response to indicate success
	response.Success = true
	response.NextIndex = len(rn.log) + 1

	return response
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
