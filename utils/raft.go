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
	HeartbeatInterval  = 500 * time.Millisecond  // Increased frequency for faster detection
	MinElectionTimeout = 1500 * time.Millisecond // Reduced for faster leader election in Docker
	MaxElectionTimeout = 3000 * time.Millisecond // Reduced for faster leader election in Docker
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type RaftNode struct {
	pb.UnimplementedRaftServer
	mu                 sync.Mutex
	id                 string
	peers              []string
	currentTerm        int
	votedFor           string
	state              State
	electionTimer      *time.Timer
	heartbeatTimer     *time.Timer
	grpcServer         *grpc.Server
	KVStore            *KVStore
	log                []*pb.LogEntry
	commitIndex        int
	lastApplied        int
	votesReceived      int
	shutdownCh         chan struct{}
	failedElections    int  // Track consecutive failed elections
	consecutiveTimeouts int // Track consecutive request timeouts
	reachablePeers     int  // Track number of reachable peers
	lastLeaderContact  time.Time // Last time we heard from a leader

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
		id:                 id,
		peers:              peers,
		currentTerm:        0,
		votedFor:           "",
		state:              State(Follower),
		grpcServer:         grpc.NewServer(),
		KVStore:            NewKVStore(),
		log:                []*pb.LogEntry{},
		commitIndex:        0,
		votesReceived:      0,
		shutdownCh:         make(chan struct{}),
		nextIndex:          make(map[string]int),
		matchIndex:         make(map[string]int),
		leaderID:           "",
		failedElections:    0,
		consecutiveTimeouts: 0,
		reachablePeers:     0,
		lastLeaderContact:  time.Now(),
	}
	rn.resetElectionTimer()
	return rn
}

func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	
	// Base timeout with randomization
	baseTimeout := MinElectionTimeout + time.Duration(rand.Intn(int(MaxElectionTimeout-MinElectionTimeout)))
	
	// If we've had too many failed elections, apply backoff to reduce election attempts
	// This helps minority partitions stabilize
	if rn.failedElections > 0 {
		backoffFactor := 1 << uint(rn.failedElections) // Exponential backoff: 2, 4, 8, etc.
		if backoffFactor > 16 {
			backoffFactor = 16 // Cap the backoff factor
		}
		baseTimeout = baseTimeout * time.Duration(backoffFactor)
		log.Printf("[%s] Term %d: Applied election backoff %dx after %d failed elections",
			rn.id, rn.currentTerm, backoffFactor, rn.failedElections)
	}
	
	rn.electionTimer = time.AfterFunc(baseTimeout, rn.startElection)
}

func (rn *RaftNode) resetHeartbeatTimer() {
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	rn.heartbeatTimer = time.AfterFunc(HeartbeatInterval, rn.sendHeartbeats)
}

// Preemptively check if we can reach a quorum of peers before starting an election
func (rn *RaftNode) canReachQuorum() bool {
	totalNodes := len(rn.peers) + 1 // Including self
	quorumSize := totalNodes/2 + 1   // Majority needed
	reachablePeers := 1             // Count self as reachable
	
	// Use a waitgroup and channel to collect results concurrently
	var wg sync.WaitGroup
	results := make(chan bool, len(rn.peers))
	
	for _, peer := range rn.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			
			// Try a quick connection to the peer
			conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithTimeout(200*time.Millisecond))
			if err != nil {
				results <- false
				return
			}
			defer conn.Close()
			results <- true
		}(peer)
	}
	
	// Wait for all connectivity checks to complete
	wg.Wait()
	close(results)
	
	// Count reachable peers
	for reachable := range results {
		if reachable {
			reachablePeers++
		}
	}
	
	// Update the reachable peers count for future reference
	rn.reachablePeers = reachablePeers
	
	// Return true if we can reach a quorum
	return reachablePeers >= quorumSize
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	
	// Check how long since we've heard from a leader
	timeSinceLeader := time.Since(rn.lastLeaderContact)
	
	// For nodes that have heard from a leader recently, don't try to become leader
	if timeSinceLeader < MinElectionTimeout {
		log.Printf("[%s] Term %d: Received leader contact %v ago, not starting election",
			rn.id, rn.currentTerm, timeSinceLeader)
		rn.resetElectionTimer() // Reschedule election
		rn.mu.Unlock()
		return
	}
	
	// If we're already a candidate, increment failed elections counter 
	if rn.state == Candidate {
		rn.failedElections++
		log.Printf("[%s] Term %d: Starting election attempt #%d", 
			rn.id, rn.currentTerm, rn.failedElections+1)
	} else {
		rn.failedElections = 0
	}
	
	// Check quorum size based on total cluster size
	totalNodes := len(rn.peers) + 1 // Including self
	quorumSize := totalNodes/2 + 1   // Majority needed
	
	// For minority partitions or after too many failed elections, do pre-election connectivity check
	// Higher failedElections count = more aggressive checks to prevent term inflation
	shouldCheckConnectivity := (rn.failedElections >= 2) || 
	                          (time.Since(rn.lastLeaderContact) > 10*MaxElectionTimeout)
	
	if shouldCheckConnectivity {
		log.Printf("[%s] Term %d: Checking connectivity before starting election (failed elections: %d)",
			rn.id, rn.currentTerm, rn.failedElections)
		
		// Release the lock during potentially lengthy network operations
		rn.mu.Unlock()
		canReachQuorum := rn.canReachQuorum()
		rn.mu.Lock()
		
		// If we can't reach a quorum, don't even try to start an election
		if !canReachQuorum {
			log.Printf("[%s] Term %d: Cannot reach quorum (%d/%d nodes), staying in follower state",
				rn.id, rn.currentTerm, rn.reachablePeers, totalNodes)
			
			// Don't change term, stay as follower
			rn.state = Follower
			
			// Set a much longer timeout before next election attempt based on failed elections
			backoffTime := MaxElectionTimeout * time.Duration(3+rn.failedElections) 
			if backoffTime > 30*time.Second {
				backoffTime = 30 * time.Second // Cap at 30 seconds
			}
			
			rn.electionTimer.Stop()
			rn.electionTimer = time.AfterFunc(backoffTime, rn.startElection)
			
			log.Printf("[%s] Term %d: Delaying election for %v due to partition detection",
				rn.id, rn.currentTerm, backoffTime)
			
			rn.mu.Unlock()
			return
		}
		
		log.Printf("[%s] Term %d: Connectivity check passed, can reach %d/%d nodes (quorum: %d)",
			rn.id, rn.currentTerm, rn.reachablePeers, totalNodes, quorumSize)
	}
	
	// If we've tried too many times, back off significantly
	if rn.failedElections >= 5 {
		log.Printf("[%s] Term %d: Too many failed elections (%d), suppressing term increment",
			rn.id, rn.currentTerm, rn.failedElections)
		
		// Stay in follower state but with longer timeout
		rn.state = Follower
		backoffTime := MaxElectionTimeout * 5 // 5x normal timeout
		rn.electionTimer.Stop()
		rn.electionTimer = time.AfterFunc(backoffTime, rn.startElection)
		
		rn.mu.Unlock()
		return
	}
	
	// Proceed with election
	rn.state = Candidate
	rn.currentTerm++ // Only increment term if we're actually starting an election
	rn.votedFor = rn.id // Vote for self
	
	currentTerm := rn.currentTerm // Store current term for async operations
	votes := 1 // Start with vote for self
	totalVotes := len(rn.peers) + 1
	quorum := totalVotes/2 + 1
	
	log.Printf("[%s] Term %d: Starting election with %d nodes (need %d votes)...",
		rn.id, rn.currentTerm, totalVotes, quorum)
	
	// Get the last log info for the vote request
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rn.log) > 0 {
		lastLogIndex = len(rn.log)
		lastLogTerm = int(rn.log[lastLogIndex-1].Term)
	}
	
	// Create a response channel to count votes
	votesCh := make(chan bool, len(rn.peers))
	votingComplete := false
	
	// Start a goroutine to manage the election result
	go func() {
		// Wait for votes to come in or timeout
		electionTimeout := MinElectionTimeout + time.Duration(rand.Intn(int(MaxElectionTimeout-MinElectionTimeout)))
		timer := time.NewTimer(electionTimeout)
		defer timer.Stop()
		
		for !votingComplete {
			select {
			case voteGranted := <-votesCh:
				if voteGranted {
					votes++
				}
				
				rn.mu.Lock()
				// Check if we're still a candidate in the same term
				if rn.state != Candidate || rn.currentTerm != currentTerm {
					votingComplete = true
					rn.mu.Unlock()
					return
				}
				
				// Check if we've received enough votes for election
				if votes >= quorum {
					rn.state = Leader
					log.Printf("[%s] Term %d: Received majority votes (%d/%d), becoming Leader",
						rn.id, rn.currentTerm, votes, totalVotes)
					
					// Initialize leader state
					for _, p := range rn.peers {
						rn.nextIndex[p] = len(rn.log) + 1
						rn.matchIndex[p] = 0
					}
					
					// Reset failed elections since we succeeded
					rn.failedElections = 0
					
					rn.resetHeartbeatTimer()
					votingComplete = true
				} else if votes + (len(rn.peers) - (votes-1)) < quorum {
					// Not enough remaining votes to reach quorum
					log.Printf("[%s] Term %d: Cannot reach quorum (%d/%d votes, need %d), election failed",
						rn.id, rn.currentTerm, votes, totalVotes, quorum)
					
					// Stay as candidate but schedule a new election with backoff
					rn.resetElectionTimer()
					votingComplete = true
				}
				rn.mu.Unlock()
				
			case <-timer.C:
				// Election timeout - we didn't get enough votes
				rn.mu.Lock()
				if rn.state == Candidate && rn.currentTerm == currentTerm {
					log.Printf("[%s] Term %d: Election timed out with %d/%d votes (needed %d)",
						rn.id, rn.currentTerm, votes, totalVotes, quorum)
					
					// Return to follower state
					rn.state = Follower
					
					// Reset election timer with backoff
					rn.resetElectionTimer()
				}
				rn.mu.Unlock()
				votingComplete = true
			}
		}
	}()
	
	// Send RequestVote RPCs to other nodes
	timeoutCount := 0
	for _, peer := range rn.peers {
		go func(peer string) {
			conn, err := grpc.Dial(peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] Failed to connect to %s: %v", rn.id, peer, err)
				
				// Record timeout to help detect partitions
				rn.mu.Lock()
				timeoutCount++
				rn.mu.Unlock()
				
				votesCh <- false
				return
			}
			defer conn.Close()
			
			client := pb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			
			req := &pb.VoteRequest{
				CandidateId:  rn.id,
				Term:         int32(currentTerm),
				LastLogIndex: int32(lastLogIndex),
				LastLogTerm:  int32(lastLogTerm),
			}
			resp, err := client.RequestVote(ctx, req)
			
			if err != nil {
				log.Printf("[%s] Term %d: Error requesting vote from %s: %v", 
					rn.id, currentTerm, peer, err)
				votesCh <- false
				return
			}
			
			// If term received is higher than ours, revert to follower
			if resp.Term > int32(currentTerm) {
				rn.mu.Lock()
				if rn.currentTerm < int(resp.Term) {
					rn.currentTerm = int(resp.Term)
					rn.state = Follower
					rn.votedFor = ""
					
					// Update last leader contact time when we hear about higher terms
					rn.lastLeaderContact = time.Now()
					
					rn.resetElectionTimer()
					log.Printf("[%s] Term %d: Discovered higher term %d, reverting to Follower",
						rn.id, currentTerm, resp.Term)
				}
				rn.mu.Unlock()
			}
			
			votesCh <- resp.VoteGranted
		}(peer)
	}
	
	rn.mu.Unlock()
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
	
	successCount := 0 // Track how many successful heartbeats we send
	totalPeers := len(rn.peers)
	
	// Create a waitgroup to track completion
	var wg sync.WaitGroup
	responses := make(chan bool, totalPeers)
	
	for _, peer := range rn.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			
			conn, err := grpc.Dial(peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] Term %d: Failed to connect to %s for heartbeat: %v", 
					rn.id, currentTerm, peer, err)
				responses <- false
				return
			}
			defer conn.Close()
			
			client := pb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			
			req := &pb.HeartbeatRequest{
				LeaderId:     rn.id,
				Term:         int32(currentTerm),
				LeaderCommit: int32(commitIndex),
				LastLogIndex: int32(lastLogIndex),
			}
			
			resp, err := client.Heartbeat(ctx, req)
			if err != nil {
				log.Printf("[%s] Term %d: Failed to send heartbeat to %s: %v", 
					rn.id, currentTerm, peer, err)
				responses <- false
				return
			}
			
			// Check if follower's term is higher than ours
			if resp.Term > int32(currentTerm) {
				rn.mu.Lock()
				if rn.currentTerm < int(resp.Term) {
					log.Printf("[%s] Term %d: Discovered higher term %d from %s, stepping down", 
						rn.id, currentTerm, resp.Term, peer)
					rn.currentTerm = int(resp.Term)
					rn.state = Follower
					rn.votedFor = ""
					rn.lastLeaderContact = time.Now()
					rn.resetElectionTimer()
				}
				rn.mu.Unlock()
				responses <- false
				return
			}
			
			// Sync logs if needed
			if resp.NeedsSync {
				rn.mu.Lock()
				
				// Check if we're still leader
				if rn.state != Leader {
					rn.mu.Unlock()
					responses <- false
					return
				}
				
				nextIdx := rn.nextIndex[peer]
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
			}
			
			responses <- true
		}(peer)
	}
	
	// Wait for all heartbeats to complete
	wg.Wait()
	close(responses)
	
	// Count successful heartbeats
	for success := range responses {
		if success {
			successCount++
		}
	}
	
	// Check if we can still maintain quorum
	rn.mu.Lock()
	
	// If we're still the leader, schedule the next heartbeat
	if rn.state == Leader {
		// Check if we have quorum (including ourselves)
		quorumSize := (totalPeers + 1) / 2 + 1
		
		if successCount + 1 < quorumSize { // +1 for self
			log.Printf("[%s] Term %d: Lost contact with majority of cluster (%d/%d), stepping down as leader",
				rn.id, rn.currentTerm, successCount, totalPeers)
			rn.state = Follower
			rn.resetElectionTimer()
		} else {
			// Still have quorum, continue as leader
			rn.resetHeartbeatTimer()
		}
	}
	
	rn.mu.Unlock()
}

// Helper method to send AppendEntries to a specific peer for log replication
func (rn *RaftNode) sendAppendEntriesToPeer(peer string, req *pb.AppendRequest) {
	conn, err := grpc.Dial(peer, grpc.WithInsecure())
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
		log.Printf("[%s] Term %d: Rejected heartbeat from %s (term %d < our term %d)",
			rn.id, rn.currentTerm, req.LeaderId, req.Term, rn.currentTerm)
		return response, nil
	}
	
	// If the leader's term is greater than ours, update our term
	if req.Term > int32(rn.currentTerm) {
		log.Printf("[%s] Term %d: Discovered higher term %d from heartbeat, updating term",
			rn.id, rn.currentTerm, req.Term)
		rn.currentTerm = int(req.Term)
		rn.state = Follower
		rn.votedFor = ""
	}
	
	// Valid heartbeat received - update leader contact time
	rn.lastLeaderContact = time.Now()
	
	// Reset failed elections since we're getting valid heartbeats
	if rn.failedElections > 0 {
		log.Printf("[%s] Term %d: Received valid heartbeat after %d failed elections, resetting counter",
			rn.id, rn.currentTerm, rn.failedElections)
		rn.failedElections = 0
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
		log.Printf("[%s] Term %d: Log needs sync with leader (leader index: %d, our index: %d)",
			rn.id, rn.currentTerm, req.LastLogIndex, len(rn.log))
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
	
	// Always include our current term in the response
	response := &VoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}
	
	// If the requester's term is smaller than ours, reject the vote
	if req.Term < rn.currentTerm {
		log.Printf("[%s] Term %d: Rejected vote for %s (term %d < our term %d)",
			rn.id, rn.currentTerm, req.CandidateId, req.Term, rn.currentTerm)
		return response
	}
	
	// If we see a higher term, update our term and revert to follower state
	if req.Term > rn.currentTerm {
		log.Printf("[%s] Term %d: Discovered higher term %d from %s, updating term and becoming follower",
			rn.id, rn.currentTerm, req.Term, req.CandidateId)
		
		// Reset failed election count when we see a higher term
		rn.failedElections = 0
		
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.state = Follower
		response.Term = rn.currentTerm
	}
	
	// We only vote if:
	// 1. We haven't voted for anyone else this term, or we've already voted for this candidate
	// 2. We haven't heard from a leader recently (to prevent vote flapping)
	timeSinceLeader := time.Since(rn.lastLeaderContact)
	
	// Don't vote if we've heard from a leader recently (unless it's a higher term)
	if timeSinceLeader < MinElectionTimeout && req.Term <= rn.currentTerm && rn.state == Follower {
		log.Printf("[%s] Term %d: Rejecting vote for %s - heard from leader %v ago",
			rn.id, rn.currentTerm, req.CandidateId, timeSinceLeader)
		return response
	}
	
	// Vote logic: Check if we haven't voted yet or already voted for this candidate
	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) {
		// Additional log check for vote safety (not required for basic implementation)
		// We could check that candidate's log is at least as up-to-date as ours
		// For simplicity, we're omitting this check now
		
		rn.votedFor = req.CandidateId
		response.VoteGranted = true
		
		// Reset election timer when granting a vote
		rn.resetElectionTimer()
		
		log.Printf("[%s] Term %d: Granted vote to %s", rn.id, rn.currentTerm, req.CandidateId)
	} else {
		log.Printf("[%s] Term %d: Rejected vote for %s (already voted for %s)",
			rn.id, rn.currentTerm, req.CandidateId, rn.votedFor)
	}
	
	return response
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
		log.Printf("[%s] Term %d: Discovered higher term %d from AppendEntries, updating term",
			rn.id, rn.currentTerm, req.Term)
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
	
	// Update last leader contact time
	rn.lastLeaderContact = time.Now()
	
	// Valid AppendEntries - reset any election-related counters
	// This helps nodes in minority partitions recognize reconnection
	if rn.failedElections > 0 {
		log.Printf("[%s] Term %d: Received valid AppendEntries after %d failed elections, resetting counter",
			rn.id, rn.currentTerm, rn.failedElections)
		rn.failedElections = 0
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