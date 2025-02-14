package main

import (
	"fmt"
	"raft-on-the-go/server"
	"log"
	"net"
)

func main() {
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
