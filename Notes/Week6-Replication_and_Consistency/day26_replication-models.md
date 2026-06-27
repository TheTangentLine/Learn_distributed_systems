# Day 26: Replication Models

## 1. Why Replicate?

- **Fault tolerance:** if one node dies, data is not lost and reads/writes can continue on another replica.
- **Read scalability:** route read traffic across multiple replicas.
- **Low-latency reads:** place replicas in geographies close to users.

## 2. The Three Models

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader/Primary
    participant F1 as Follower 1
    participant F2 as Follower 2

    Note over L,F2: Single-leader (most common)
    C->>L: Write
    L-->>F1: Replicate (async or sync)
    L-->>F2: Replicate (async or sync)
    L-->>C: Ack

    Note over L,F2: Multi-leader (multi-datacenter)
    Note over L,F2: Each datacenter has its own leader;\nleaders replicate to each other

    Note over L,F2: Leaderless (Dynamo-style)
    Note over L,F2: Client writes to W nodes directly;\nreads from R nodes; R+W > N ensures overlap
```

### Single-Leader (Master-Slave)

- All writes go to the leader. Reads can go to leader or followers.
- Leader replicates changes to followers via a **write-ahead log (WAL) stream** or **row-based binlog**.
- **Pro:** simple conflict model — the leader is the authority.
- **Con:** write throughput limited to one machine. Failover takes 10–30 seconds.

_Examples: PostgreSQL streaming replication, MySQL async replication, MongoDB replica sets._

### Multi-Leader

- Multiple nodes accept writes. Each replicates to the others asynchronously.
- **Pro:** writes survive network partitions between datacenters.
- **Con:** conflicting writes on the same key from different leaders are possible and require explicit conflict resolution.

_Examples: CockroachDB (multi-region), Active-Active DynamoDB global tables._

### Leaderless (Dynamo-style)

- Any replica accepts writes. The client or a coordinator writes to W replicas and reads from R replicas.
- Uses the quorum rule `R + W > N` to guarantee at least one overlap between read and write sets.
- **Pro:** no single point of failure for writes. Continues operating with any minority of nodes down.
- **Con:** conflict resolution required (vector clocks + application-level merging or LWW).

_Examples: Amazon DynamoDB, Apache Cassandra, Riak._

---

## Hands-on Assignment (Go)

We implement a single-leader pair: a primary HTTP server that asynchronously replicates writes to a replica via a background goroutine.

### Step 1: Create `main.go`

```go
package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string
	name string
}

func (s *Store) Set(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = val
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

type WriteOp struct {
	key, val string
}

func main() {
	primary := &Store{data: make(map[string]string), name: "primary"}
	replica := &Store{data: make(map[string]string), name: "replica"}

	// Replication channel: primary queues writes, background goroutine sends to replica
	replCh := make(chan WriteOp, 100)

	// Async replication goroutine (simulates network delay)
	go func() {
		for op := range replCh {
			time.Sleep(200 * time.Millisecond) // simulate async lag
			replica.Set(op.key, op.val)
			fmt.Printf("  [repl] replicated %s=%s\n", op.key, op.val)
		}
	}()

	// Primary: accepts writes
	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")
		primary.Set(key, val)
		replCh <- WriteOp{key, val}
		fmt.Fprintf(w, "primary wrote %s=%s\n", key, val)
	})

	// Primary: reads from primary (always fresh)
	http.HandleFunc("/read-primary", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		v, _ := primary.Get(key)
		fmt.Fprintf(w, "primary: %s=%q\n", key, v)
	})

	// Replica: reads from replica (may be stale)
	http.HandleFunc("/read-replica", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		v, _ := replica.Get(key)
		fmt.Fprintf(w, "replica: %s=%q\n", key, v)
	})

	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}
```

### Step 2: Run and observe replication lag

```bash
go run main.go
```

```bash
# Write to primary
curl "localhost:8080/write?key=name&val=Kha"

# Read from both immediately
curl "localhost:8080/read-primary?key=name"  # → "Kha" (fresh)
curl "localhost:8080/read-replica?key=name"  # → "" (stale for ~200ms)

# Wait 300ms, read from replica again
sleep 0.3
curl "localhost:8080/read-replica?key=name"  # → "Kha" (now replicated)
```

---

## Review

1. In a single-leader setup, the primary goes down. How does the system decide which follower should become the new leader? What data could be lost during this process?

2. Why does multi-leader replication across datacenters add write conflict risk that single-leader does not have?
