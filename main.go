package main

import (
	"fmt"
	"raft-on-the-go/server"
	"sync"
	"time"
)

func main() {
	nodeCount := 5
	ports := []string{"5001", "5002", "5003", "5004", "5005"}
	var wg sync.WaitGroup

	nodes := make([]*server.Node, nodeCount)

	// Initialize nodes
	for i := 0; i < nodeCount; i++ {
		peers := append([]string{}, ports...)
		peers = append(peers[:i], peers[i+1:]...)
		nodes[i] = server.NewNode(fmt.Sprintf("Node%d", i+1), ports[i], peers)
	}

	// Start nodes
	for _, node := range nodes {
		wg.Add(1)
		go func(n *server.Node) {
			defer wg.Done()
			n.Start()
		}(node)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("All nodes started.")

	wg.Wait()
}
