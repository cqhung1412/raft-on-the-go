package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	raftpb "raft-on-the-go/proto"
	"raft-on-the-go/server"
	"strconv"

	"google.golang.org/grpc"
)

// runServer starts a Raft server node on the first available port within the range 5001 to 5005.
// It sequentially checks each port for availability, and upon finding one, it computes an HTTP inspection port
// by adding 1000 to the gRPC port, initializes a new node (excluding the chosen port from its peers),
// and concurrently launches both the node’s gRPC and HTTP inspection services. The function then blocks indefinitely;
// if no free port is detected in the range, it logs a fatal error and terminates the execution.
func runServer() {
	ports := []string{"5001", "5002", "5003", "5004", "5005"}

	// Kiểm tra từng cổng trong dải
	for i, port := range ports {
		// address := fmt.Sprintf("localhost:%s", port) // Thay đổi từ %d thành %s cho phù hợp với chuỗi

		// Kiểm tra nếu cổng này không bị chiếm dụng
		listener, err := net.Listen("tcp", ":"+port)
		if err != nil {
			// Nếu cổng đã bị chiếm dụng, tiếp tục kiểm tra cổng khác
			fmt.Printf("Port %s used, check next port...\n", port)
			continue
		}

		// Cổng trống, gán process vào cổng này
		if err := listener.Close(); err != nil {
			log.Fatalf("Closing listener %v failed: %v\n", i, err)
		}
		

		// Tính toán cổng cho HTTP inspect: cộng thêm 1000 (ví dụ: 5001 -> 6001)
		grpcPort, err := strconv.Atoi(port)
		if err != nil {
			log.Fatalf("Invalid port %s", port)
		}
		inspectPort := strconv.Itoa(grpcPort + 1000)

		
		// Tạo một node mới
		nodeName := fmt.Sprintf("Node%d", i+1)
		fmt.Printf("[%s] Starting process on port %s...\n", nodeName, port)
		peers := append([]string{}, ports...)
		peers = append(peers[:i], peers[i+1:]...) // Loại bỏ cổng hiện tại khỏi peers
		newNode := server.NewNode(nodeName, ports[i], peers)

		// Tạo một goroutine để bắt đầu chạy node
		go func() {
			newNode.Start()
		}()

		go func() {
			newNode.StartHTTP(inspectPort)
		}()

		// Nếu cổng trống, chương trình sẽ không tiếp tục kiểm tra các cổng còn lại
		// và dừng lại tại đây. Bạn có thể thay đổi điều này nếu muốn thử với các cổng khác.
		select {} // Giữ chương trình chạy vĩnh viễn
	}

	// Nếu không tìm thấy cổng trống nào
	log.Fatal("Không tìm thấy cổng trống trong dải 5001-5005. Đảm bảo rằng các cổng này chưa bị chiếm dụng.")
}


// runClient connects to a Raft leader node via gRPC and sends an AppendEntries request.
// 
// It establishes a gRPC connection to the leader at the given port, constructs an AppendRequest with a log entry that includes
// the specified term and a predefined command ("key1=value1"), and sends the request using the Raft service client. The function
// prints the leader's response, indicating the term and whether the request was successful. If an error occurs during connection
// or while sending the request, it logs a fatal error and terminates the program.
func runClient(leaderPort string, term int32) {
	// Kết nối tới Leader node qua gRPC
	conn, err := grpc.Dial("localhost:"+leaderPort, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Không thể kết nối tới Leader: %v", err)
	}
	defer conn.Close()

	// Tạo một client gRPC
	client := raftpb.NewRaftClient(conn)

	// Tạo yêu cầu AppendEntries
	entries := []*raftpb.LogEntry{
		{
			Term:    term, // giả sử term có kiểu int32
			Command: "key1=value1",
		},
	}
	req := &raftpb.AppendRequest{
		Term:    term,
		Entries: entries,
	}

	// Gửi yêu cầu AppendEntries
	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		log.Fatalf("Error when send AppendEntries: %v", err)
	}

	fmt.Printf("Leader response: Term=%d, Success=%v\n", resp.Term, resp.Success)
}


// main is the entry point for the Raft-based gRPC application. It parses command-line flags to determine whether to run as a client or as a server. If the "--client" flag is set, main runs in client mode by connecting to a designated leader and sending an AppendEntries request; otherwise, it starts a server node.
func main() {
	// Thêm flag cho client
	clientFlag := flag.Bool("client", false, "Run client to send request")
	leaderPort := flag.String("leader", "5001", "Leader's port")
	term := flag.Int("term", 1, "Leader's term")

	flag.Parse()

	// Nếu flag --client được sử dụng, chạy client
	if *clientFlag {
		runClient(*leaderPort, int32(*term))
		return
	}

	// Nếu không có flag --client, chạy server
	runServer()
}