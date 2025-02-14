package server

import (
	"context"
	"fmt"
	"log"
	"net"
	pb "raft-on-the-go/proto"

	"raft-on-the-go/utils"

	"google.golang.org/grpc"
)

type Node struct {
	pb.RaftServer

	id       string
	port     string
	peers    []string
	RaftNode *utils.RaftNode
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
	// Cập nhật dữ liệu vào KVStore (lưu trữ log entries)
	n.RaftNode.KVStore.SyncData(req.Entries)
	
	response := n.RaftNode.HandleAppendEntries(&utils.AppendRequest{
		Term:         int(req.Term),
		LeaderId:     req.LeaderId,
		Entries:      req.Entries,
		LeaderCommit: int(req.LeaderCommit),
	})

	return &pb.AppendResponse{
		Term:    int32(response.Term),
		Success: response.Success,
	}, nil
}

// Heartbeat RPC Implementation
func (n *Node) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	log.Printf("[%s] Term %d: Received heartbeat from Leader %s ", n.id, req.Term, req.LeaderId)	

	return n.RaftNode.ReceiveHeartbeat(req)
}

func NewNode(id, port string, peers []string) *Node {
	return &Node{
		id:       id,
		port:     port,
		peers:    peers,
		RaftNode: utils.NewRaftNode(id, peers),
	}
}

func (n *Node) Start() {
	lis, err := net.Listen("tcp", ":"+n.port)
	if err != nil {
		log.Fatalf("%s failed to listen: %v", n.id, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, n)

	fmt.Printf("[%s] Running on port %s\n", n.id, n.port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("%s failed to serve: %v", n.id, err)
	}
}
