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

	// Additional stability constants
	LeaderStabilityTimeout = 10000 * time.Millisecond // Don't start elections if we've seen a leader recently (increased to 10s)
	PostPartitionStability = 15000 * time.Millisecond // Additional stability period after reconnection
	MaxTermGrowthRate      = 2                        // Maximum number of terms a node can grow above cluster
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type RaftNode struct {
	pb.UnimplementedRaftServer
	mu                  sync.Mutex
	id                  string
	peers               []string
	currentTerm         int
	votedFor            string
	state               State
	electionTimer       *time.Timer
	heartbeatTimer      *time.Timer
	grpcServer          *grpc.Server
	KVStore             *KVStore
	log                 []*pb.LogEntry
	commitIndex         int
	lastApplied         int
	votesReceived       int
	shutdownCh          chan struct{}
	failedElections     int       // Track consecutive failed elections
	consecutiveTimeouts int       // Track consecutive request timeouts
	reachablePeers      int       // Track number of reachable peers
	lastLeaderContact   time.Time // Last time we heard from a leader
	lastReconnection    time.Time // Last time we detected a network reconnection
	stableLeader        bool      // True if we believe there's a stable leader

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
		id:                  id,
		peers:               peers,
		currentTerm:         0,
		votedFor:            "",
		state:               State(Follower),
		grpcServer:          grpc.NewServer(),
		KVStore:             NewKVStore(),
		log:                 []*pb.LogEntry{},
		commitIndex:         0,
		lastApplied:         0,
		votesReceived:       0,
		shutdownCh:          make(chan struct{}),
		nextIndex:           make(map[string]int),
		matchIndex:          make(map[string]int),
		leaderID:            "",
		failedElections:     0,
		consecutiveTimeouts: 0,
		reachablePeers:      0,
		lastLeaderContact:   time.Time{}, // Initialize to zero time to indicate no leader contact yet
		lastReconnection:    time.Time{}, // Zero time initially (no reconnection yet)
		stableLeader:        false,
	}
	// Don't start the election timer immediately - it will be started explicitly
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

	// If we're the leader, set a much longer timeout
	if rn.state == Leader {
		baseTimeout = baseTimeout * 10 // Make leader timeouts much longer
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
	quorumSize := totalNodes/2 + 1  // Majority needed
	reachablePeers := 1             // Count self as reachable
	previousReachablePeers := rn.reachablePeers

	// Use a waitgroup and channel to collect results concurrently
	var wg sync.WaitGroup
	results := make(chan bool, len(rn.peers))

	// Try to detect if there's already an active leader
	foundLeader := false
	leaderTerm := 0
	foundLeaderID := ""

	for _, peer := range rn.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			// Try a quick connection to the peer
			conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithTimeout(500*time.Millisecond))
			if err != nil {
				results <- false
				return
			}
			defer conn.Close()

			// Also check if this peer believes it's a leader or knows of a leader
			client := pb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			// Try sending a heartbeat request to see if this peer responds
			resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
				Term:     int32(rn.currentTerm),
				LeaderId: "", // No leader ID since we're checking
			})

			if err == nil && resp != nil {
				results <- true // The peer is reachable

				// If the peer has a leader ID, note it
				if resp.LeaderId != "" {
					rn.mu.Lock()
					foundLeader = true
					leaderTerm = int(resp.Term)
					foundLeaderID = resp.LeaderId
					rn.mu.Unlock()
				}

				// If the peer has a higher term, note it
				if resp.Term > int32(rn.currentTerm) {
					rn.mu.Lock()
					foundLeader = true
					leaderTerm = int(resp.Term)
					rn.mu.Unlock()
				}
			} else {
				results <- false // Connection error
			}
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

	// If we can now reach significantly more peers than before, this might indicate
	// a network reconnection/partition healing
	rn.mu.Lock()
	defer rn.mu.Unlock()

	networkReconnected := false

	if previousReachablePeers > 0 && reachablePeers > previousReachablePeers &&
		reachablePeers >= quorumSize && previousReachablePeers < quorumSize {
		// We've gone from a minority to a majority - network likely healed
		log.Printf("[%s] Term %d: Network reconnection detected! Peers reachable: %d→%d (quorum: %d)",
			rn.id, rn.currentTerm, previousReachablePeers, reachablePeers, quorumSize)
		rn.lastReconnection = time.Now()
		networkReconnected = true
	}

	// Update the reachable peers count for future reference and make sure it's accurate
	rn.reachablePeers = reachablePeers

	// Log a warning if in a minority partition for debugging purposes
	if reachablePeers < quorumSize {
		log.Printf("[%s] Term %d: WARNING - In minority partition (%d/%d nodes reachable, quorum: %d)",
			rn.id, rn.currentTerm, reachablePeers, totalNodes, quorumSize)
	}

	// If we found a leader with higher term, apply it now
	if foundLeader && leaderTerm >= rn.currentTerm {
		log.Printf("[%s] Term %d: Found leader (ID: %s) with term %d during quorum check, updating",
			rn.id, rn.currentTerm, foundLeaderID, leaderTerm)

		if leaderTerm > rn.currentTerm {
			rn.currentTerm = leaderTerm
		}

		if foundLeaderID != "" {
			rn.leaderID = foundLeaderID
		}

		rn.state = Follower
		rn.votedFor = ""

		// Update last leader contact time
		rn.lastLeaderContact = time.Now()

		if networkReconnected {
			// If we've just reconnected and immediately found a leader,
			// be extra stable to prevent term inflation
			log.Printf("[%s] Term %d: Post-reconnection leader stability enforced",
				rn.id, rn.currentTerm)
		}

		// We shouldn't try to become a leader if there's already one with a higher/equal term
		return false
	}

	// Return true if we can reach a quorum
	return reachablePeers >= quorumSize
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	// Check if we're the leader already
	if rn.state == Leader {
		// Just reset the timer and continue
		rn.resetElectionTimer()
		rn.mu.Unlock()
		return
	}

	// Check how long since we've heard from a leader
	timeSinceLeader := time.Since(rn.lastLeaderContact)

	// For nodes that have heard from a leader recently, don't try to become leader
	if timeSinceLeader < LeaderStabilityTimeout {
		log.Printf("[%s] Term %d: Received leader contact %v ago (< %v), delaying election",
			rn.id, rn.currentTerm, timeSinceLeader, LeaderStabilityTimeout)
		rn.resetElectionTimer() // Reschedule election
		rn.mu.Unlock()
		return
	}

	// Check if we've recently experienced a network reconnection
	// If we have, we should be much more conservative about starting elections
	timeSinceReconnection := time.Since(rn.lastReconnection)
	if !rn.lastReconnection.IsZero() && timeSinceReconnection < PostPartitionStability {
		log.Printf("[%s] Term %d: Network recently reconnected %v ago (< %v), enforcing post-partition stability",
			rn.id, rn.currentTerm, timeSinceReconnection, PostPartitionStability)

		// If there's a known leader and it's not us, be very conservative
		if rn.leaderID != "" && rn.leaderID != rn.id {
			log.Printf("[%s] Term %d: Known leader exists (%s), avoiding disruptive election after reconnection",
				rn.id, rn.currentTerm, rn.leaderID)

			// Set an extra-long timeout to give existing leader time to stabilize
			backoffTime := PostPartitionStability // Use the entire stability period
			rn.electionTimer.Stop()
			rn.electionTimer = time.AfterFunc(backoffTime, rn.startElection)

			rn.mu.Unlock()
			return
		}
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
	quorumSize := totalNodes/2 + 1  // Majority needed

	// Always do a connectivity check right after restoring connections or if in doubt
	shouldCheckConnectivity := (rn.failedElections >= 1) || // Always check after first failure
		(!rn.lastReconnection.IsZero() && time.Since(rn.lastReconnection) < PostPartitionStability*2) ||
		(time.Since(rn.lastLeaderContact) > 5*MaxElectionTimeout)

	if shouldCheckConnectivity {
		log.Printf("[%s] Term %d: Checking connectivity before starting election (failed elections: %d, reconnected: %v)",
			rn.id, rn.currentTerm, rn.failedElections, !rn.lastReconnection.IsZero())

		// Release the lock during potentially lengthy network operations
		rn.mu.Unlock()
		canReachQuorum := rn.canReachQuorum()
		rn.mu.Lock()

		// If we can't reach a quorum, don't start an election and revert any term increases
		if !canReachQuorum && rn.reachablePeers >= quorumSize {
			// Return to follower state
			wasCandidate := rn.state == Candidate
			rn.state = Follower

			// If this is a repeat failure in minority partition, revert term increment
			// to prevent term inflation in isolated nodes
			if wasCandidate && rn.failedElections > 1 {
				// Revert the term increment that would happen below
				log.Printf("[%s] Term %d: In minority partition, preventing term increment",
					rn.id, rn.currentTerm)
			}

			log.Printf("[%s] Term %d: Cannot reach quorum (%d/%d nodes), staying in follower state",
				rn.id, rn.currentTerm, rn.reachablePeers, totalNodes)

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
	if rn.failedElections >= 3 {
		log.Printf("[%s] Term %d: Too many failed elections (%d), reverting to follower without term increment",
			rn.id, rn.currentTerm, rn.failedElections)

		// Stay in follower state but with longer timeout
		rn.state = Follower
		backoffTime := MaxElectionTimeout * 5 // 5x normal timeout
		rn.electionTimer.Stop()
		rn.electionTimer = time.AfterFunc(backoffTime, rn.startElection)

		rn.mu.Unlock()
		return
	}

	// Record the pre-election term for possible rollback
	// oldTerm := rn.currentTerm

	// Proceed with election - clear any existing leader ID
	rn.leaderID = ""

	// Always check connectivity status right before starting election
	// Release the lock temporarily to avoid deadlock during network operations
	rn.mu.Unlock()
	canReachQuorumResult := rn.canReachQuorum()
	rn.mu.Lock()

	// Check if we can actually get enough votes before incrementing term
	totalVotes := len(rn.peers) + 1
	quorum := totalVotes/2 + 1

	// If we know we're in a minority partition, don't increment term
	if !canReachQuorumResult && rn.reachablePeers < quorum {
		log.Printf("[%s] Term %d: In minority partition (%d/%d nodes), preventing term increment (canReachQuorum=%v)",
			rn.id, rn.currentTerm, rn.reachablePeers, totalVotes, canReachQuorumResult)
		// Transition to candidate state, but don't increment term
		rn.state = Candidate
		rn.votedFor = rn.id // Vote for self
	} else {
		// Transition to candidate state
		rn.state = Candidate
		rn.currentTerm++    // Increment term for this election attempt
		rn.votedFor = rn.id // Vote for self
	}

	currentTerm := rn.currentTerm // Store current term for async operations
	votes := 1                    // Start with vote for self

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
					// Create a no-op entry to commit after becoming leader
					// This is a critical step from the Raft paper - it ensures log consistency
					noOpEntry := &pb.LogEntry{
						Term:    int32(rn.currentTerm),
						Command: "no-op", // Special marker for leadership establishment
					}

					// Append the no-op entry to our log
					rn.log = append(rn.log, noOpEntry)

					// Switch to leader state
					rn.state = Leader
					rn.leaderID = rn.id

					log.Printf("[%s] Term %d: Received majority votes (%d/%d), becoming Leader",
						rn.id, rn.currentTerm, votes, totalVotes)

					// Initialize leader state
					for _, p := range rn.peers {
						rn.nextIndex[p] = len(rn.log) + 1
						rn.matchIndex[p] = 0
					}

					// Reset failed elections since we succeeded
					rn.failedElections = 0
					rn.lastLeaderContact = time.Now()

					// Stop the election timer and start the heartbeat timer
					if rn.electionTimer != nil {
						rn.electionTimer.Stop()
					}
					rn.resetHeartbeatTimer()
					votingComplete = true

				} else if votes+(len(rn.peers)-(votes-1)) < quorum {
					// Not enough remaining votes to reach quorum
					log.Printf("[%s] Term %d: Cannot reach quorum (%d/%d votes, need %d), election failed",
						rn.id, rn.currentTerm, votes, totalVotes, quorum)

					// We don't need to revert term since we don't increment it in minority partitions
					if rn.reachablePeers < quorum {
						log.Printf("[%s] Term %d: Election failed in minority partition",
							rn.id, rn.currentTerm)
					}

					// Return to follower state
					rn.state = Follower
					rn.resetElectionTimer()
					votingComplete = true
				}
				rn.mu.Unlock()

			case <-timer.C:
				// Election timeout - we didn't get enough votes
				rn.mu.Lock()
				if rn.state == Candidate && rn.currentTerm == currentTerm {
					// We don't need to check for minority partition since we're not incrementing term in that case
					// Just log if we're in a minority partition
					if rn.reachablePeers < quorum {
						log.Printf("[%s] Term %d: Election timed out in minority partition",
							rn.id, rn.currentTerm)
					}
					log.Printf("[%s] Term %d: Election timed out with %d/%d votes (needed %d)",
						rn.id, rn.currentTerm, votes, totalVotes, quorum)

					// Return to follower state
					rn.state = Follower

					// Reset election timer with appropriate backoff
					rn.resetElectionTimer()
				}
				rn.mu.Unlock()
				votingComplete = true
			}
		}
	}()

	// Send RequestVote RPCs to other nodes
	for _, peer := range rn.peers {
		go func(peer string) {
			// Try to connect to the peer
			conn, err := grpc.Dial(peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] Failed to connect to %s: %v", rn.id, peer, err)

				// Count as not granted
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
		quorumSize := (totalPeers+1)/2 + 1

		if successCount+1 < quorumSize { // +1 for self
			log.Printf("[%s] Term %d: Lost contact with majority of cluster (%d/%d), stepping down as leader",
				rn.id, rn.currentTerm, successCount, totalPeers)
			rn.state = Follower
			rn.resetElectionTimer()
		} else {
			// Still have quorum, continue as leader
			rn.resetHeartbeatTimer()

			// Update last contact time to maintain leadership
			rn.lastLeaderContact = time.Now()
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
	// Only update if the log entry is from our current term OR we're committing a previous entry
	if majorityIndex > rn.commitIndex && len(rn.log) >= majorityIndex {
		// Find the highest log entry from the current term that we can commit
		// (according to the Raft paper, we can only commit entries from the current term)
		newCommitIndex := majorityIndex

		// If the highest majority-replicated entry is from a previous term,
		// we need to be careful about committing it
		if int(rn.log[newCommitIndex-1].Term) != rn.currentTerm {
			// Check if we're committing entries from previous terms
			// We can only commit entries from previous terms if they're
			// covered by a newer entry from our term that's been replicated
			// to a majority of servers.

			// Find the highest log entry from the current term
			highestCurrentTermEntry := -1
			for i := len(rn.log) - 1; i >= 0; i-- {
				if int(rn.log[i].Term) == rn.currentTerm {
					highestCurrentTermEntry = i + 1 // 1-indexed
					break
				}
			}

			// If we found a log entry from the current term that's replicated to a majority,
			// we can commit all entries up to this point
			if highestCurrentTermEntry > 0 && highestCurrentTermEntry <= majorityIndex {
				// We can commit all entries up to and including this one
				newCommitIndex = highestCurrentTermEntry
				log.Printf("[%s] Term %d: Found entry from current term at index %d, committing up to here",
					rn.id, rn.currentTerm, newCommitIndex)
			} else {
				// We can't commit entries from previous terms without a newer entry from our term
				log.Printf("[%s] Term %d: Can't commit entry at index %d (term %d) because there's no newer entry from current term",
					rn.id, rn.currentTerm, majorityIndex, rn.log[majorityIndex-1].Term)
				return
			}
		}

		// Update the commit index
		rn.updateCommitIndexInternal(newCommitIndex)
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
		LeaderId:     rn.leaderID, // Return our known leader ID
	}

	// If the leader's term is less than ours, reject heartbeat
	if req.Term < int32(rn.currentTerm) {
		log.Printf("[%s] Term %d: Rejected heartbeat from %s (term %d < our term %d)",
			rn.id, rn.currentTerm, req.LeaderId, req.Term, rn.currentTerm)
		return response, nil
	}

	// If this is the first heartbeat after a long silence, it might indicate reconnection
	timeSinceContact := time.Since(rn.lastLeaderContact)
	if timeSinceContact > 5*HeartbeatInterval && req.LeaderId != "" {
		log.Printf("[%s] Term %d: Received heartbeat after %v silence - possible reconnection detected",
			rn.id, rn.currentTerm, timeSinceContact)

		// If we haven't already recorded a recent reconnection
		if rn.lastReconnection.IsZero() || time.Since(rn.lastReconnection) > PostPartitionStability {
			// This could be a reconnection event
			rn.lastReconnection = time.Now()
		}
	}

	// If the leader's term is greater than ours, update our term
	if req.Term > int32(rn.currentTerm) {
		log.Printf("[%s] Term %d: Discovered higher term %d from heartbeat, updating term",
			rn.id, rn.currentTerm, req.Term)
		rn.currentTerm = int(req.Term)
		rn.state = Follower
		rn.votedFor = ""
	}

	// Valid heartbeat received - update leader contact time only if node is initialized
	// If not initialized, we want to preserve the zero time value
	if !rn.lastLeaderContact.IsZero() {
		rn.lastLeaderContact = time.Now()
	}

	// Reset failed elections since we're getting valid heartbeats
	if rn.failedElections > 0 {
		log.Printf("[%s] Term %d: Received valid heartbeat after %d failed elections, resetting counter",
			rn.id, rn.currentTerm, rn.failedElections)
		rn.failedElections = 0
	}

	// Store the leader ID
	if req.LeaderId != "" {
		rn.leaderID = req.LeaderId
		response.LeaderId = req.LeaderId // Echo back the leader ID
	}

	// Reset election timer upon receiving valid heartbeat
	rn.resetElectionTimer()

	// Check if our log is behind the leader's
	response.Success = true

	// If leader has more entries than us, or if our commit index is behind
	// leader's commit index, or if our lastApplied is behind commit index, we need to sync
	if req.LastLogIndex > int32(len(rn.log)) ||
		req.LeaderCommit > int32(rn.commitIndex) ||
		rn.lastApplied < rn.commitIndex {
		response.NeedsSync = true
		log.Printf("[%s] Term %d: Log needs sync with leader (leader index: %d, our index: %d, leader commit: %d, our commit: %d, our applied: %d)",
			rn.id, rn.currentTerm, req.LastLogIndex, len(rn.log), req.LeaderCommit, rn.commitIndex, rn.lastApplied)
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

	// Check for recent network reconnection - be extra conservative
	// about voting right after reconnection
	if !rn.lastReconnection.IsZero() && time.Since(rn.lastReconnection) < PostPartitionStability {
		// If we know of an existing leader, don't vote for anyone else
		// This helps maintain stability after reconnection
		if rn.leaderID != "" && rn.leaderID != req.CandidateId {
			log.Printf("[%s] Term %d: Rejecting vote for %s - recent reconnection (%v ago) with known leader %s",
				rn.id, rn.currentTerm, req.CandidateId, time.Since(rn.lastReconnection), rn.leaderID)
			return response
		}
	}

	// We only vote if:
	// 1. We haven't voted for anyone else this term, or we've already voted for this candidate
	// 2. We haven't heard from a leader recently (to prevent vote flapping)
	// timeSinceLeader := time.Since(rn.lastLeaderContact)

	// Don't vote if we've heard from a leader recently (unless it's a higher term)
	// if timeSinceLeader < LeaderStabilityTimeout && req.Term <= rn.currentTerm && rn.state == Follower {
	// 	log.Printf("[%s] Term %d: Rejecting vote for %s - heard from leader %v ago",
	// 		rn.id, rn.currentTerm, req.CandidateId, timeSinceLeader)
	// 	return response
	// }

	// Don't grant vote if our log is more recent than the candidate's
	upToDate := true

	// If we have log entries, check if our log is more up-to-date
	if len(rn.log) > 0 {
		// Get our last log term
		ourLastLogTerm := int(rn.log[len(rn.log)-1].Term)
		ourLastLogIndex := len(rn.log)

		// Per Raft paper section 5.4.1:
		// If the logs have last entries with different terms, then
		// the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer
		// is more up-to-date.
		if ourLastLogTerm > req.LastLogTerm {
			upToDate = false // Our log has higher term, reject vote
			log.Printf("[%s] Term %d: Rejecting vote for %s - our log term %d > candidate's term %d",
				rn.id, rn.currentTerm, req.CandidateId, ourLastLogTerm, req.LastLogTerm)
		} else if ourLastLogTerm == req.LastLogTerm && ourLastLogIndex > req.LastLogIndex {
			upToDate = false // Same term but our log is longer, reject vote
			log.Printf("[%s] Term %d: Rejecting vote for %s - same term but our log longer (%d > %d)",
				rn.id, rn.currentTerm, req.CandidateId, ourLastLogIndex, req.LastLogIndex)
		}
	}

	// Vote logic: Check if we haven't voted yet or already voted for this candidate,
	// AND if the candidate's log is at least as up-to-date as ours
	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) && upToDate {
		// Check stability condition: don't grant vote immediately after reconnection
		// unless to the current leader or if it's been a long time since leader contact
		unstableVote := !rn.lastReconnection.IsZero() &&
			time.Since(rn.lastReconnection) < PostPartitionStability &&
			rn.leaderID != "" &&
			rn.leaderID != req.CandidateId &&
			time.Since(rn.lastLeaderContact) < LeaderStabilityTimeout*2

		if unstableVote {
			log.Printf("[%s] Term %d: Rejecting vote for %s - prioritizing stability after reconnection",
				rn.id, rn.currentTerm, req.CandidateId)
			return response
		}

		rn.votedFor = req.CandidateId
		response.VoteGranted = true

		// Reset election timer when granting a vote
		rn.resetElectionTimer()

		log.Printf("[%s] Term %d: Granted vote to %s", rn.id, rn.currentTerm, req.CandidateId)
	} else if !upToDate {
		log.Printf("[%s] Term %d: Rejected vote for %s (candidate log not up-to-date)",
			rn.id, rn.currentTerm, req.CandidateId)
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

	// If this is the first AppendEntries after a long silence, it might indicate reconnection
	timeSinceContact := time.Since(rn.lastLeaderContact)
	if timeSinceContact > 5*HeartbeatInterval {
		log.Printf("[%s] Term %d: Received AppendEntries after %v silence - possible reconnection detected",
			rn.id, rn.currentTerm, timeSinceContact)

		// If we haven't already recorded a recent reconnection
		if rn.lastReconnection.IsZero() || time.Since(rn.lastReconnection) > PostPartitionStability {
			// This could be a reconnection event
			rn.lastReconnection = time.Now()
		}
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
		if req.LeaderId == "" || (rn.leaderID != "" && req.LeaderId != rn.leaderID && !rn.lastReconnection.IsZero() && time.Since(rn.lastReconnection) < PostPartitionStability) {
			log.Printf("[%s] Term %d: Rejecting AppendEntries from unexpected leader %s (current known leader: %s)",
				rn.id, rn.currentTerm, req.LeaderId, rn.leaderID)
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

	// Store the leader ID - this is critical after network reconnection to establish stable leadership
	if req.LeaderId != "" {
		// If it's a different leader than what we know, but we've just reconnected,
		// note this as significant leadership change
		if rn.leaderID != "" && req.LeaderId != rn.leaderID &&
			!rn.lastReconnection.IsZero() && time.Since(rn.lastReconnection) < PostPartitionStability {
			log.Printf("[%s] Term %d: Leadership change detected after reconnection: %s → %s",
				rn.id, rn.currentTerm, rn.leaderID, req.LeaderId)
		}

		rn.leaderID = req.LeaderId
	}

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
		oldCommitIndex := rn.commitIndex
		rn.commitIndex = newCommitIndex
		log.Printf("[%s] Term %d: Updated commitIndex from %d to %d",
			rn.id, rn.currentTerm, oldCommitIndex, rn.commitIndex)

		// Áp dụng các entry từ lastApplied+1 đến commitIndex
		for rn.lastApplied < rn.commitIndex {
			rn.lastApplied++

			if rn.lastApplied > len(rn.log) {
				log.Printf("[%s] Term %d: ERROR - lastApplied %d exceeds log length %d",
					rn.id, rn.currentTerm, rn.lastApplied, len(rn.log))
				rn.lastApplied = len(rn.log) // Correct the index
				break
			}

			entry := rn.log[rn.lastApplied-1] // Vì rn.lastApplied tính theo 1-indexed

			// Handle special no-op entry (used to establish leadership)
			if entry.Command == "no-op" {
				log.Printf("[%s] Term %d: Applied no-op entry at index %d",
					rn.id, rn.currentTerm, rn.lastApplied)
				continue
			}

			// Process regular key-value entries
			parts := strings.SplitN(entry.Command, "=", 2)
			if len(parts) == 2 {
				rn.KVStore.Set(parts[0], parts[1])
				log.Printf("[%s] Term %d: Applied entry at index %d, key=%s, value=%s",
					rn.id, rn.currentTerm, rn.lastApplied, parts[0], parts[1])
			} else {
				// If it's not a key-value pair, log it but don't update the KV store
				log.Printf("[%s] Term %d: Applied non-KV entry at index %d: %s",
					rn.id, rn.currentTerm, rn.lastApplied, entry.Command)
			}
		}

		// After applying entries, consider extending the leader lease
		if rn.state == Leader {
			// If we successfully committed entries, extend our leadership tenure
			rn.lastLeaderContact = time.Now()
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

// InitializeNode starts the election timer and makes the node ready for elections
// This should be called after all nodes are created to ensure proper synchronization
func (rn *RaftNode) InitializeNode() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	log.Printf("[%s] Initializing node and starting election timer", rn.id)

	// Set the lastLeaderContact time to now to mark the node as initialized
	rn.lastLeaderContact = time.Now()

	// Start the election timer now that we're initialized
	rn.resetElectionTimer()
}

// IsInitialized returns true if the node has been initialized
func (rn *RaftNode) IsInitialized() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// If lastLeaderContact is the zero time, the node is not initialized
	return !rn.lastLeaderContact.IsZero()
}

// StepDownToFollower forces a node to step down to follower state
// This is a convenience method for nodes that discover they should no longer be leader
func (rn *RaftNode) StepDownToFollower(newTerm int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Only step down if the new term is higher
	if newTerm > rn.currentTerm {
		log.Printf("[%s] Term %d: Stepping down to follower due to higher term %d",
			rn.id, rn.currentTerm, newTerm)

		// Update to the new term
		rn.currentTerm = newTerm

		// Reset vote
		rn.votedFor = ""

		// Change state to follower
		rn.state = Follower

		// Update leader contact time to prevent immediate re-election
		rn.lastLeaderContact = time.Now()

		// Reset election timer
		rn.resetElectionTimer()
	} else if rn.state != Follower {
		// Even without a higher term, we may need to step down
		log.Printf("[%s] Term %d: Stepping down to follower state",
			rn.id, rn.currentTerm)

		rn.state = Follower
		rn.resetElectionTimer()
	}
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
