package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	raftpb "raft-on-the-go/proto"
	"raft-on-the-go/server"

	"google.golang.org/grpc"
)

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

		// Nếu cổng trống, chương trình sẽ không tiếp tục kiểm tra các cổng còn lại
		// và dừng lại tại đây. Bạn có thể thay đổi điều này nếu muốn thử với các cổng khác.
		select {} // Giữ chương trình chạy vĩnh viễn
	}

	// Nếu không tìm thấy cổng trống nào
	log.Fatal("Không tìm thấy cổng trống trong dải 5001-5005. Đảm bảo rằng các cổng này chưa bị chiếm dụng.")
}


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
	entries := []string{"key1=value1", "key2=value2", "key3=value3"}
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