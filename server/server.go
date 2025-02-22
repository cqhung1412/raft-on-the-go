package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	pb "raft-on-the-go/proto"
	raftpb "raft-on-the-go/proto"
	"raft-on-the-go/utils"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type Node struct {
	pb.RaftServer

	id         string
	port       string
	peers      []string
	RaftNode   *utils.RaftNode
	grpcServer *grpc.Server
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
	if req.LeaderId != "" {
		n.RaftNode.KVStore.SyncData(req.Entries)
	}

	response := n.RaftNode.HandleAppendEntries(&utils.AppendRequest{
		Term:         int(req.Term),
		LeaderId:     req.LeaderId,
		Entries:      req.Entries,
		LeaderCommit: int(req.LeaderCommit),
	})

	// Nếu node này là Leader, tiến hành replicate entries tới các follower
	if n.RaftNode.GetState() == utils.Leader {
		// Nếu leader nhận được request mà LeaderId rỗng, tự gán giá trị cho nó.
		if req.LeaderId == "" {
			req.LeaderId = n.id
		}

		go n.replicateEntries(req)
	}

	return &pb.AppendResponse{
		Term:    int32(response.Term),
		Success: response.Success,
	}, nil
}

// Thêm phương thức replicateEntries cho Node
func (n *Node) replicateEntries(req *pb.AppendRequest) {
	var wg sync.WaitGroup
	successCount := 1 // Leader tự đếm thành công vì leader đã append entry vào log
	responseCh := make(chan bool, len(n.peers))

	// Cập nhật LeaderCommit trong request theo commitIndex hiện tại của leader
	req.LeaderCommit = int32(len(n.RaftNode.GetLog()))

	// Gửi AppendEntries tới từng follower
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			conn, err := grpc.Dial("localhost:"+peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] Failed to connect to follower %s: %v", n.id, peer, err)
				responseCh <- false
				return
			}
			defer conn.Close()

			follower := pb.NewRaftClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()

			resp, err := follower.AppendEntries(ctx, req)
			if err != nil {
				log.Printf("[%s] Error replicating to follower %s: %v", n.id, peer, err)
				responseCh <- false
			} else {
				log.Printf("[%s] Follower %s responded: Term=%d, Success=%v", n.id, peer, resp.Term, resp.Success)
				responseCh <- resp.Success
			}
		}(peer)
	}

	wg.Wait()
	close(responseCh)

	for success := range responseCh {
		if success {
			successCount++
		}
	}

	// Nếu đa số các node (leader + follower) thành công, cập nhật commitIndex
	if successCount > len(n.peers)/2 {
		newCommitIndex := len(n.RaftNode.GetLog())
		n.RaftNode.UpdateCommitIndex(newCommitIndex)
	}
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

// Start chạy gRPC server
func (n *Node) Start() {
	lis, err := net.Listen("tcp", ":"+n.port)
	if err != nil {
		log.Fatalf("%s failed to listen: %v", n.id, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, n)
	n.grpcServer = grpcServer

	fmt.Printf("[%s] Running on port %s\n", n.id, n.port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("%s failed to serve: %v", n.id, err)
	}
}

// inspectHandler trả về thông tin node ở định dạng JSON
func (n *Node) inspectHandler(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"node_id":      n.id,
		"current_term": n.RaftNode.GetCurrentTerm(),
		"state":        n.RaftNode.GetState().StateString(),
		"log_entries":  n.RaftNode.GetLog(),
		"commit_index": n.RaftNode.GetCommitIndex(),
		"last_applied": n.RaftNode.GetLastApply(),
		"kv_store":     n.RaftNode.KVStore.GetStore(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (n *Node) shutdownHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	data := map[string]string{"message": "Unable to shutdown server"}
	w.Header().Set("Content-Type", "application/json")

	log.Printf("Initiating graceful shutdown for %s %s...", n.RaftNode.GetState().StateString(), n.id)
	timer := time.AfterFunc(10*time.Second, func() {
		log.Println("Server could not stop gracefully in time. Performing force shutdown.")
		n.grpcServer.Stop()
		data["message"] = "Server stopped forcefully"
	})
	defer timer.Stop()

	n.RaftNode.Shutdown()
	n.grpcServer.GracefulStop()
	log.Println("Server stopped gracefully.")
	data["message"] = "Server stopped gracefully"

	json.NewEncoder(w).Encode(data)
}

func (n *Node) appendEntryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var req pb.AppendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if n.RaftNode.GetState() != utils.Leader {
		http.Error(w, "Node is not a leader", http.StatusForbidden)
		return
	}

	ctx := context.Background()
	response, _ := n.AppendEntries(ctx, &raftpb.AppendRequest{
		Term:         int32(n.RaftNode.GetCurrentTerm()),
		LeaderId:     n.id,
		Entries:      req.Entries,
		LeaderCommit: int32(n.RaftNode.GetCommitIndex()),
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&pb.AppendResponse{
		Term:    int32(response.Term),
		Success: response.Success,
	})
}

// StartHTTP chạy một HTTP server trên cổng được cung cấp, phục vụ endpoint /inspect
func (n *Node) StartHTTP(httpPort string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/inspect", n.inspectHandler)
	mux.HandleFunc("/shutdown", n.shutdownHandler)
	mux.HandleFunc("/append", n.appendEntryHandler)
	log.Printf("[%s] HTTP inspect endpoint running on port %s", n.id, httpPort)
	if err := http.ListenAndServe(":"+httpPort, mux); err != nil {
		log.Fatalf("[%s] HTTP server error: %v", n.id, err)
	}
}
