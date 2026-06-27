# Day 35: Gossip Protocols

## 1. The Problem with Centralized State

If every node reports its status to a central coordinator, the coordinator becomes a bottleneck and SPOF. If we broadcast state to all N nodes, each update costs O(N) messages per update — expensive at scale.

**Gossip protocols** (also called epidemic protocols) offer a middle ground: `O(log N)` rounds to converge, with no single coordinator.

## 2. How Gossip Works

Each round, every node:
1. Picks `f` random peers (fanout, typically 2–3).
2. Sends its current state to those peers.
3. Merges the state received from peers into its own.

Because each round doubles the number of informed nodes, convergence is logarithmic: after `log2(N)` rounds, all N nodes know the information.

```
Round 0: 1 node has new info
Round 1: 1 × 2 = 2 nodes
Round 2: 2 × 2 = 4 nodes
Round 3: 4 × 2 = 8 nodes
...
Round log2(N): all N nodes
```

## 3. SWIM Failure Detection

SWIM (Scalable Weakly-consistent Infection-style Process Group Membership) uses gossip to spread membership changes (join, leave, failure):

1. Node A sends a direct ping to Node B.
2. If B doesn't respond within a timeout, A sends indirect pings through `k` random nodes ("ping-req"). If any of them can reach B, B is healthy.
3. If B still doesn't respond, A marks B as **suspect** and gossips the suspect state.
4. If B doesn't refute the suspicion within a grace period, it is marked **failed** and that state is gossiped.

This avoids false failures due to a single network path being congested, while still detecting real failures efficiently.

---

## Hands-on Assignment (Go)

We simulate 5 goroutines gossiping a `state map[string]string`. We measure how many rounds until all 5 nodes have all keys.

### Step 1: Set up the project

```bash
mkdir dist-sys-day35
cd dist-sys-day35
go mod init day35
```

### Step 2: Create `gossip.go`

```go
package main

import (
	"fmt"
	"math/rand"
	"sync"
)

const (
	nodeCount = 5
	fanout    = 2
)

type Node struct {
	id    int
	mu    sync.Mutex
	state map[string]string
}

func NewNode(id int) *Node {
	return &Node{
		id:    id,
		state: make(map[string]string),
	}
}

func (n *Node) SetState(key, value string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state[key] = value
}

func (n *Node) GetSnapshot() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	snap := make(map[string]string, len(n.state))
	for k, v := range n.state {
		snap[k] = v
	}
	return snap
}

func (n *Node) Merge(incoming map[string]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, v := range incoming {
		n.state[k] = v
	}
}

func (n *Node) StateSize() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.state)
}

func gossipRound(nodes []*Node) {
	for _, n := range nodes {
		// Pick `fanout` random peers
		peerIndices := rand.Perm(len(nodes))
		sent := 0
		for _, idx := range peerIndices {
			if idx == n.id { continue }
			snap := n.GetSnapshot()
			nodes[idx].Merge(snap)
			sent++
			if sent >= fanout { break }
		}
	}
}

func allConverged(nodes []*Node, totalKeys int) bool {
	for _, n := range nodes {
		if n.StateSize() < totalKeys {
			return false
		}
	}
	return true
}

func main() {
	nodes := make([]*Node, nodeCount)
	for i := range nodes {
		nodes[i] = NewNode(i)
	}

	// Seed each node with unique keys
	totalKeys := 0
	for i, n := range nodes {
		key := fmt.Sprintf("node%d-info", i)
		val := fmt.Sprintf("value-from-%d", i)
		n.SetState(key, val)
		totalKeys++
		fmt.Printf("Node %d seeded with key %q\n", i, key)
	}

	fmt.Printf("\n=== Starting gossip (%d nodes, fanout=%d) ===\n", nodeCount, fanout)
	for round := 1; round <= 20; round++ {
		gossipRound(nodes)

		allDone := allConverged(nodes, totalKeys)
		fmt.Printf("Round %2d: key counts = ", round)
		for _, n := range nodes {
			fmt.Printf("%d ", n.StateSize())
		}
		if allDone {
			fmt.Printf("← ✅ converged!\n")
			fmt.Printf("Converged in %d rounds (log2(%d) ≈ %.1f)\n",
				round, nodeCount, float64(round))
			return
		}
		fmt.Println()
	}
	fmt.Println("Did not converge in 20 rounds")
}
```

### Step 3: Run it

```bash
go run gossip.go
```

Expected: convergence in ~3 rounds for 5 nodes (log2(5) ≈ 2.3).

### Step 4: Scale it up

Change `nodeCount` to 100. How many rounds does it take? Then try 1000. Verify that the round count grows as `log2(N)`, not linearly.

### Step 5: Weekend Project

Use Redis Pub/Sub (from Day 31) to decouple your Day 5 chat app components. Instead of the gRPC server broadcasting directly:

1. When a client calls `SendMessage`, the server publishes to a Redis channel `chat:room:1`.
2. A separate goroutine (or separate process) subscribes to that channel and broadcasts to connected gRPC streams.

This way the gRPC server and the broadcast router are decoupled — you can scale them independently.

---

## Your Next Step

You have mastered all the building blocks: communication, system models, time, consensus, storage, replication, and patterns. Next week is the **Grand Finale**: paper case studies, system design interviews, and building a real distributed key-value store.
