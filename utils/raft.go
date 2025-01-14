package utils

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type RaftNode struct {
	// raftpb.UnimplementedRaftServer
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
		timeout := time.Duration(rand.Intn(2000)+2000) * time.Millisecond
		electionReset := time.After(timeout)

		select {
		case <-electionReset:
			rn.mu.Lock()
			if rn.state == "follower" || rn.state == "candidate" {
				log.Printf("%s starting election for term %d", rn.id, rn.currentTerm+1)
				rn.startElection()
			}
			rn.mu.Unlock()
		}
	}
}

func (rn *RaftNode) ResetElectionTimer() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
}

func (rn *RaftNode) ReceiveHeartbeat(term int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if term >= rn.currentTerm {
		rn.currentTerm = term
		rn.state = "follower"
		rn.ResetElectionTimer()
		log.Printf("%s received heartbeat from term %d", rn.id, term)
	}
}

func (rn *RaftNode) startElection() {
	rn.state = "candidate"
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.votesReceived = 1
	log.Printf("%s became candidate for term %d", rn.id, rn.currentTerm)

	for _, peer := range rn.peers {
		go func(peer string) {
			// Send RequestVote RPCs
			log.Printf("[Unimplemented] %s is requesting vote from %s", rn.id, peer)
		}(peer)
	}

	time.Sleep(2 * time.Second)
	if rn.votesReceived > len(rn.peers)/2 {
		rn.state = "leader"
		log.Printf("%s became leader for term %d", rn.id, rn.currentTerm)
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

	rn.state = "follower"
	rn.currentTerm = req.Term
	rn.log = append(rn.log, req.Entries...)

	log.Printf("%s received AppendEntries from %s", rn.id, req.LeaderId)

	return &AppendResponse{
		Term:    rn.currentTerm,
		Success: true,
	}
}
