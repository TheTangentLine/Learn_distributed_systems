# Day 20: Byzantine Faults & Weekend Project — Heartbeat Leader Election

## 1. Byzantine Faults

All week we have assumed **crash-stop** or **crash-recovery** faults: a node either works correctly or stops entirely. **Byzantine faults** are far worse — a node may:

- Send different values to different peers for the same message.
- Replay old messages.
- Lie about its state.
- Collude with other faulty nodes.

### The Byzantine Generals Problem

Lamport, Shostak, Pease (1982) formulated this as the Byzantine Generals Problem: several army generals must coordinate an attack, but some may be traitors sending contradictory orders. The honest generals must still reach agreement.

**Result:** to tolerate `f` Byzantine nodes, you need at least `3f + 1` total nodes.

With `f=1` traitor, you need 4 nodes minimum. With only 3 nodes and 1 traitor:
- Traitor tells General A "attack", tells General B "retreat".
- A and B each hear 2 conflicting messages and cannot determine which is correct.

### When do you need Byzantine fault tolerance?

- **Blockchains:** validators are untrusted third parties who may be financially incentivised to cheat (Tendermint, HotStuff).
- **Multi-party computation:** participants from different organizations.
- **Internal clusters:** almost never. If you trust your own hardware, Raft (crash-recovery) is sufficient.

---

## 2. Weekend Project — Heartbeat-Based Leader Election

We build a simpler, practical version of leader election without implementing full Raft. Three Go HTTP servers ping each other. If a server misses N consecutive pings from the current leader, it declares itself the new leader.

This is how many real systems (Redis Sentinel, simple cluster managers) work in practice.

## Hands-on Assignment (Go)

### Step 1: Set up the project

```bash
mkdir dist-sys-day20
cd dist-sys-day20
go mod init day20
```

### Step 2: Create `node.go`

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type Status struct {
	NodeID   string `json:"node_id"`
	IsLeader bool   `json:"is_leader"`
	Term     int    `json:"term"`
}

type Node struct {
	mu       sync.Mutex
	id       string
	peers    []string
	isLeader bool
	term     int
	lastPing time.Time
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isLeader = true
	n.term++
	fmt.Printf("✅ [%s] Became LEADER for term %d\n", n.id, n.term)
}

func (n *Node) stepDown() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.isLeader {
		fmt.Printf("⬇️  [%s] Stepped down from leader\n", n.id)
	}
	n.isLeader = false
	n.lastPing = time.Now()
}

func (n *Node) handlePing(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	n.lastPing = time.Now()
	if n.isLeader {
		n.isLeader = false // higher-term leader appeared
	}
	n.mu.Unlock()
	fmt.Fprintf(w, "ack")
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	s := Status{NodeID: n.id, IsLeader: n.isLeader, Term: n.term}
	n.mu.Unlock()
	json.NewEncoder(w).Encode(s)
}

func (n *Node) sendPings() {
	for {
		n.mu.Lock()
		isLeader := n.isLeader
		n.mu.Unlock()
		if isLeader {
			for _, peer := range n.peers {
				go func(p string) {
					_, err := http.Get("http://" + p + "/ping")
					if err != nil {
						fmt.Printf("⚠️  [%s] ping to %s failed\n", n.id, p)
					}
				}(peer)
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (n *Node) watchdog() {
	for {
		time.Sleep(300 * time.Millisecond)
		n.mu.Lock()
		isLeader := n.isLeader
		lastPing := n.lastPing
		n.mu.Unlock()

		if !isLeader && time.Since(lastPing) > 600*time.Millisecond {
			fmt.Printf("⚡ [%s] No ping from leader — starting election\n", n.id)
			n.becomeLeader()
		}
	}
}

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node1"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	peers := []string{}
	if os.Getenv("PEERS") != "" {
		for _, p := range splitEnv("PEERS") {
			peers = append(peers, p)
		}
	}

	n := &Node{
		id:       nodeID,
		peers:    peers,
		lastPing: time.Now(),
	}

	// Node 1 starts as leader
	if nodeID == "node1" {
		n.isLeader = true
		n.term = 1
		fmt.Printf("✅ [%s] Starting as initial leader\n", nodeID)
	}

	http.HandleFunc("/ping", n.handlePing)
	http.HandleFunc("/status", n.handleStatus)

	go n.sendPings()
	go n.watchdog()

	fmt.Printf("[%s] listening on :%s\n", nodeID, port)
	http.ListenAndServe(":"+port, nil)
}

func splitEnv(key string) []string {
	val := os.Getenv(key)
	if val == "" {
		return nil
	}
	result := []string{}
	start := 0
	for i := 0; i < len(val); i++ {
		if val[i] == ',' {
			result = append(result, val[start:i])
			start = i + 1
		}
	}
	result = append(result, val[start:])
	return result
}
```

### Step 3: Run three nodes in separate terminals

```bash
# Terminal 1 — initial leader
NODE_ID=node1 PORT=8081 PEERS=localhost:8082,localhost:8083 go run node.go

# Terminal 2
NODE_ID=node2 PORT=8082 PEERS=localhost:8081,localhost:8083 go run node.go

# Terminal 3
NODE_ID=node3 PORT=8083 PEERS=localhost:8081,localhost:8082 go run node.go
```

### Step 4: Test leader election

Check current status:
```bash
curl localhost:8081/status
curl localhost:8082/status
curl localhost:8083/status
```

Kill the leader:
```bash
# Press Ctrl+C in Terminal 1
```

Within 600ms, one of the remaining nodes should detect the missing pings and declare itself the new leader. Check status again.

---

## Your Next Step

You have now implemented the two hardest building blocks of distributed systems: **ordering** (Lamport clocks, Week 3) and **agreement** (consensus, Raft, this week). Next week we use these foundations to tackle the storage problem: how do you store data across hundreds of nodes? That is **partitioning and consistent hashing**.
