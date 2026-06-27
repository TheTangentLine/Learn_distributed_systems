# Day 28: Quorums

## 1. The Formula

In a leaderless replication system with N replicas, a **quorum** ensures that a write set and a read set always overlap:

```
W + R > N
```

Where:
- **N** = total number of replicas
- **W** = number of replicas that must acknowledge a write
- **R** = number of replicas that must respond to a read

If W + R > N, at least one replica in every read set has the most recent write.

### Common configurations (N=3)

| W | R | Durability | Read speed | Write speed | Notes |
|---|---|-----------|-----------|-------------|-------|
| 3 | 1 | Highest | Fast | Slow | Strong consistency |
| 2 | 2 | High | Medium | Medium | Typical balanced |
| 1 | 3 | Low | Slow | Fast | Not useful (W+R=4=N+1) |
| 1 | 1 | Very low | Fastest | Fastest | AP, stale reads possible |

With W=2, R=2, N=3: write touches 2 nodes, read touches 2 nodes → at least 1 overlap.

## 2. Sloppy Quorums and Hinted Handoff

In a strict quorum, if W nodes are not available, the write fails. DynamoDB and Cassandra offer a **sloppy quorum**: if the intended W replica nodes are down, the write goes to any W available nodes. A **hint** is stored noting that the data belongs to the unavailable node. When that node recovers, the hint is "handed off" — the data is replicated to its correct home.

- **Advantage:** improves write availability during partial failures.
- **Disadvantage:** a read quorum may not see the sloppy write because the correct W nodes were never written to. Consistency is weaker during the fault window.

---

## Hands-on Assignment (Go)

We simulate leaderless quorum writes and reads across 3 in-memory HTTP servers.

### Step 1: Set up the project

```bash
mkdir dist-sys-day28
cd dist-sys-day28
go mod init day28
```

### Step 2: Create `node.go` (a simple KV store node)

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
)

type KV struct {
	mu   sync.RWMutex
	data map[string]string
}

var store = &KV{data: make(map[string]string)}

func main() {
	port := os.Getenv("PORT")
	if port == "" { port = "9001" }

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")
		store.mu.Lock()
		store.data[key] = val
		store.mu.Unlock()
		fmt.Fprintf(w, "ok")
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		store.mu.RLock()
		v := store.data[key]
		store.mu.RUnlock()
		json.NewEncoder(w).Encode(map[string]string{"value": v})
	})

	fmt.Printf("Node listening on :%s\n", port)
	http.ListenAndServe(":"+port, nil)
}
```

### Step 3: Create `coordinator.go` (handles W and R)

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

var nodes = []string{"localhost:9001", "localhost:9002", "localhost:9003"}

const N, W, R = 3, 2, 2

func quorumWrite(key, val string) int {
	var acks int32
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			_, err := http.Get(fmt.Sprintf("http://%s/set?key=%s&val=%s", n, key, val))
			if err == nil {
				atomic.AddInt32(&acks, 1)
			} else {
				fmt.Printf("  ⚠️  write to %s failed: %v\n", n, err)
			}
		}(node)
	}
	wg.Wait()
	return int(acks)
}

func quorumRead(key string) (string, int) {
	type result struct {
		value string
		ok    bool
	}
	results := make(chan result, N)
	for _, node := range nodes {
		go func(n string) {
			resp, err := http.Get(fmt.Sprintf("http://%s/get?key=%s", n, key))
			if err != nil {
				results <- result{ok: false}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			var m map[string]string
			json.Unmarshal(body, &m)
			results <- result{value: m["value"], ok: true}
		}(node)
	}

	successes := 0
	val := ""
	for i := 0; i < N; i++ {
		r := <-results
		if r.ok {
			successes++
			if r.value != "" { val = r.value }
		}
	}
	return val, successes
}

func startCoordinator() {
	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")
		acks := quorumWrite(key, val)
		if acks >= W {
			fmt.Fprintf(w, "write succeeded with %d/%d acks\n", acks, N)
		} else {
			http.Error(w, fmt.Sprintf("write failed: only %d/%d acks", acks, W), 500)
		}
	})

	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val, responses := quorumRead(key)
		if responses >= R {
			fmt.Fprintf(w, "value=%q (from %d/%d replicas)\n", val, responses, N)
		} else {
			http.Error(w, fmt.Sprintf("read failed: only %d/%d responses", responses, R), 500)
		}
	})

	fmt.Println("Coordinator on :8080")
	http.ListenAndServe(":8080", nil)
}
```

### Step 4: Run the experiment

```bash
# Terminal 1,2,3 — nodes
PORT=9001 go run node.go
PORT=9002 go run node.go
PORT=9003 go run node.go

# Terminal 4 — coordinator
go run coordinator.go
```

```bash
# Write via quorum
curl "localhost:8080/write?key=x&val=hello"

# Read via quorum
curl "localhost:8080/read?key=x"

# Kill one node (Ctrl+C Terminal 3), retry write and read
# W=2, R=2 should still succeed with 2 of 3 nodes alive
```

---

## Review

1. You set W=1, R=1 (N=3). A client writes to Node A. A different client immediately reads from Node B. Can the read see stale data? Why?

2. With W=2, R=2, N=3: one node is down. A write succeeds (acked by 2 nodes). A read then goes to the 2 nodes that received the write. Is the read guaranteed to be fresh? What if the 2 read nodes are different from the 2 write nodes?
