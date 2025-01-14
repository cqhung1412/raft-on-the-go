package server

import (
	"fmt"
	"log"
	"net"
	pb "raft-on-the-go/proto"
	"raft-on-the-go/utils"

	"google.golang.org/grpc"
)

type Node struct {
	pb.UnimplementedRaftServer

	id       string
	port     string
	peers    []string
	RaftNode *utils.RaftNode
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

	fmt.Printf("%s running on port %s\n", n.id, n.port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("%s failed to serve: %v", n.id, err)
	}
}
