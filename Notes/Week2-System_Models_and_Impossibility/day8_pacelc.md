# Day 8: The PACELC Theorem

## 1. The Problem with CAP

CAP only talks about behavior **during a partition**. But partitions are rare. Most of the time your cluster is healthy — and you still have to make trade-offs.

**PACELC** (Daniel Abadi, 2012) extends CAP to cover the normal case:

> **P**artition: choose between **A**vailability and **C**onsistency.
> **E**lse (no partition): choose between **L**atency and **C**onsistency.

## 2. The Else Branch

When there is no partition, a write still has to replicate to other nodes before you can guarantee consistency. That replication takes time — latency. You must choose:

- **EC (Else Consistent):** wait for replicas to confirm before responding to the client. Correct but slower.
- **EL (Else Low-latency):** respond immediately after writing to one node, replicate asynchronously. Fast but potentially stale on reads.

### System classification

| System | Partition | Else | Label |
|--------|-----------|------|-------|
| etcd / Raft | CP | EC | PC/EC |
| DynamoDB (default) | AP | EL | PA/EL |
| Cassandra (W=QUORUM) | CP | EC | PC/EC |
| Cassandra (W=ONE) | AP | EL | PA/EL |
| MySQL async replica | AP | EL | PA/EL |
| MySQL semi-sync replica | CP | EC | PC/EC |
| CockroachDB | CP | EC | PC/EC |

The EL/EC dimension is often **more practically relevant** than CAP because partitions are rare but latency pressure is constant.

---

## Hands-on Assignment (Go)

We extend the Day 7 two-node setup with a configurable `syncDelay` to observe the latency vs consistency trade-off directly.

### Step 1: Create `main.go`

```go
package main

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Node struct {
	mu   sync.RWMutex
	data map[string]string
}

func (n *Node) Set(key, value string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.data[key] = value
}

func (n *Node) Get(key string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.data[key]
}

var (
	primary  = &Node{data: make(map[string]string)}
	replica  = &Node{data: make(map[string]string)}
	syncDelay = 0 * time.Millisecond // tunable
)

func main() {
	// Sync goroutine: primary replicates to replica with a configurable delay
	go func() {
		for {
			time.Sleep(syncDelay + 50*time.Millisecond)
			primary.mu.RLock()
			for k, v := range primary.data {
				replica.Set(k, v)
			}
			primary.mu.RUnlock()
		}
	}()

	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")
		start := time.Now()
		primary.Set(key, val)
		// EC mode: wait for sync before responding
		if r.URL.Query().Get("mode") == "ec" {
			time.Sleep(syncDelay)
			replica.Set(key, val)
		}
		fmt.Fprintf(w, "wrote %s=%s in %v\n", key, val, time.Since(start))
	})

	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		fmt.Fprintf(w, "replica says %s=%q\n", key, replica.Get(key))
	})

	http.HandleFunc("/delay", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		syncDelay = time.Duration(ms) * time.Millisecond
		fmt.Fprintf(w, "sync delay set to %dms\n", ms)
	})

	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}
```

### Step 2: Experiment — EL mode (low latency, stale reads)

```bash
go run main.go

# Set delay to 500ms to simulate a slow replica
curl "localhost:8080/delay?ms=500"

# Write in EL mode (async)
curl "localhost:8080/write?key=name&val=Kha"

# Read immediately from replica — stale!
curl "localhost:8080/read?key=name"

# Wait 600ms and read again — now fresh
sleep 0.6
curl "localhost:8080/read?key=name"
```

### Step 3: Experiment — EC mode (consistent, higher latency)

```bash
# Write in EC mode (waits for replica sync before responding)
time curl "localhost:8080/write?key=name&val=NewKha&mode=ec"
# Notice the write takes ~500ms instead of ~0ms

# Read immediately — now fresh
curl "localhost:8080/read?key=name"
```

---

## Review

1. You are designing a leaderboard for a mobile game. Millions of players update their scores per minute. Do you choose PA/EL or PC/EC? Justify your answer.

2. You are designing the balance display for a bank transfer UI. After a user submits a transfer, they are redirected to their account page. Do you choose PA/EL or PC/EC for the read on that page? Why?
