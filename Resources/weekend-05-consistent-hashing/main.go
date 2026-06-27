package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	totalKeys = 1000
	vnodes    = 150
)

func measureMovement(before, after map[string]string) (moved, total int) {
	for k, bNode := range before {
		total++
		if after[k] != bNode {
			moved++
		}
	}
	return
}

func simulateHotKey(ring *Ring, hotKey string, requests int) map[string]int {
	counts := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := ring.GetNode(hotKey)
			mu.Lock()
			counts[node]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	return counts
}

func simulateHotKeyMitigated(ring *Ring, hotKey string, requests, buckets int) map[string]int {
	counts := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var counter int64
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := atomic.AddInt64(&counter, 1)
			shardedKey := fmt.Sprintf("%s:%02d", hotKey, n%int64(buckets))
			node := ring.GetNode(shardedKey)
			mu.Lock()
			counts[node]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	return counts
}

func printDistribution(label string, counts map[string]int, total int) {
	fmt.Printf("\n=== %s ===\n", label)
	for node, c := range counts {
		fmt.Printf("  %-8s: %d (%.1f%%)\n", node, c, float64(c)/float64(total)*100)
	}
}

func main() {
	ring := NewRing(vnodes)
	ring.AddNode("NodeA")
	ring.AddNode("NodeB")
	ring.AddNode("NodeC")

	// Sample key routing
	fmt.Println("=== Initial routing (3 nodes, vnodes=150) ===")
	samples := []string{"user:1", "user:2", "product:42", "order:100", "session:xyz"}
	for _, k := range samples {
		fmt.Printf("  %-20s → %s\n", k, ring.GetNode(k))
	}

	// Record routing before adding a 4th node
	before := make(map[string]string, totalKeys)
	for i := 0; i < totalKeys; i++ {
		k := fmt.Sprintf("key-%04d", i)
		before[k] = ring.GetNode(k)
	}

	// Add NodeD and measure movement
	ring.AddNode("NodeD")

	after := make(map[string]string, totalKeys)
	for i := 0; i < totalKeys; i++ {
		k := fmt.Sprintf("key-%04d", i)
		after[k] = ring.GetNode(k)
	}

	moved, _ := measureMovement(before, after)
	fmt.Printf("\n=== Adding NodeD: %d/%d keys moved (%.1f%%)\n",
		moved, totalKeys, float64(moved)/float64(totalKeys)*100)
	fmt.Printf("    Expected ~%.1f%% (1/N = 1/4)\n", 100.0/4.0)

	// Distribution after NodeD
	dist := map[string]int{}
	for i := 0; i < totalKeys; i++ {
		k := fmt.Sprintf("key-%04d", i)
		dist[ring.GetNode(k)]++
	}
	printDistribution("Distribution after NodeD added", dist, totalKeys)

	// Hot-spot demonstration
	const reqCount = 1000
	hotKey := "celebrity:taylor-swift"

	fmt.Printf("\n=== Hot spot: all %d requests hit key %q ===\n", reqCount, hotKey)
	hotDist := simulateHotKey(ring, hotKey, reqCount)
	for node, c := range hotDist {
		fmt.Printf("  %-8s: %d requests\n", node, c)
	}

	fmt.Printf("\n=== Hot spot mitigated with 50 buckets ===\n")
	mitigated := simulateHotKeyMitigated(ring, hotKey, reqCount, 50)
	for node, c := range mitigated {
		fmt.Printf("  %-8s: %d requests (%.1f%%)\n", node, c, float64(c)/float64(reqCount)*100)
	}
}
