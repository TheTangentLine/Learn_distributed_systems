# Day 25: Rebalancing & Hot Spots

## 1. What Happens When a Node Joins or Leaves?

Rebalancing is the process of redistributing keys when the cluster topology changes. There are two strategies:

### Fixed partitions

Create far more partitions than nodes (e.g., 1000 partitions for 10 nodes). Each node is responsible for ~100 partitions. When a new node joins, it takes a few partitions from each existing node. The partition itself does not split — only its ownership changes.

- **Advantage:** the total number of partitions never changes, which simplifies routing tables.
- **Disadvantage:** you must choose the number of partitions upfront. Too few → large partitions, too slow to move. Too many → overhead of tracking partition ownership.
- **Used by:** Elasticsearch (shards), Kafka (partitions).

### Dynamic partitions

Start with one partition. When it grows too large, split it into two. When a partition shrinks, merge with its neighbor.

- **Advantage:** partition sizes stay bounded automatically.
- **Disadvantage:** splitting can create a thundering herd on the new partition if requests are not redirected quickly.
- **Used by:** HBase, Google Spanner.

## 2. Hot Spots

A hot spot is when one partition or node receives a disproportionate fraction of traffic. Common causes:

1. **Sequential writes:** timestamps, auto-increment IDs all go to the "latest" partition.
2. **Celebrity data:** one user (e.g., BeyoncÃ© on Twitter) has 100 million followers. Every post write fans out to 100 million feeds. One user key becomes a hot key.
3. **Skewed hash:** if the hash function is weak, many keys collide to the same position.

**Mitigation strategies:**

- Add a random 2-digit suffix to the key: `user:123:47`. Distributes writes across 100 partitions. Reads must now query all 100 and merge — acceptable for writes-heavy paths.
- Rate limit hot keys at the application layer.
- Cache hot keys in an in-process cache (a read-through cache means only cache misses hit the storage layer).

---

## Hands-on Assignment (Go)

We add a 4th node to the Day 24 ring, measure key movement, and simulate a hot spot.

### Step 1: Copy your Day 24 code

```bash
cp -r dist-sys-day24 dist-sys-day25
cd dist-sys-day25
```

### Step 2: Add hot spot simulation to `main.go`

```go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
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

func simulateHotKey(ring *Ring, hotKey string, requests int) map[string]int64 {
	counts := map[string]int64{}
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

func simulateHotKeyMitigated(ring *Ring, hotKey string, requests int, buckets int) map[string]int64 {
	counts := map[string]int64{}
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

func main() {
	ring := NewRing(150)
	ring.AddNode("NodeA")
	ring.AddNode("NodeB")
	ring.AddNode("NodeC")

	const total = 1000
	before := map[string]string{}
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("key-%04d", i)
		before[k] = ring.GetNode(k)
	}

	ring.AddNode("NodeD")

	after := map[string]string{}
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("key-%04d", i)
		after[k] = ring.GetNode(k)
	}

	moved, _ := measureMovement(before, after)
	fmt.Printf("Keys moved after adding NodeD: %d/%d (%.1f%%)\n",
		moved, total, float64(moved)/float64(total)*100)

	// Hot spot demo
	const reqCount = 1000
	hotKey := "celebrity:taylor-swift"

	fmt.Printf("\n=== Hot spot: all %d requests hit key %q ===\n", reqCount, hotKey)
	dist := simulateHotKey(ring, hotKey, reqCount)
	for node, c := range dist {
		fmt.Printf("  %-8s: %d requests\n", node, c)
	}

	fmt.Printf("\n=== Hot spot mitigated with 50 buckets ===\n")
	dist2 := simulateHotKeyMitigated(ring, hotKey, reqCount, 50)
	for node, c := range dist2 {
		fmt.Printf("  %-8s: %d requests\n", node, c)
	}
}
```

### Step 3: Run it

```bash
go run ring.go main.go
```

Observe:
- Without mitigation: 100% of `reqCount` requests go to a single node.
- With 50 buckets: the load spreads roughly evenly across all 4 nodes (~250 each).

---

## Your Next Step

You now understand how to distribute data across nodes and what happens when the cluster changes. Next week we tackle the other side: what happens when multiple nodes hold **copies** of the same data? That is **replication** — and it brings back all the consistency problems from Week 2 in a new form.
