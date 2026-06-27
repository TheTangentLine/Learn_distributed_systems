# Day 29: Conflict Resolution

## 1. When Conflicts Happen

A conflict occurs when two nodes accept concurrent writes to the same key — i.e., neither write happened-before the other (they are concurrent in the vector clock sense). This is inevitable in:

- Leaderless replication (any replica can accept a write)
- Multi-leader replication (both leaders accept writes)

You must decide: which value wins?

## 2. Last Write Wins (LWW)

Each write is tagged with a timestamp. On conflict, the write with the higher timestamp survives.

- **Simple** to implement.
- **Lossy:** the lower-timestamp write is silently discarded. If two clients write concurrently (within the clock drift window), one write simply disappears — no error, no warning.
- **Cassandra** uses LWW by default. DynamoDB uses LWW in its simplest mode.
- **Clock skew is dangerous here:** if Node A's clock is 10ms ahead, any write from Node A in a concurrent window silently wins over Node B's write.

## 3. CRDTs (Conflict-free Replicated Data Types)

A CRDT is a data structure whose merge operation is:

- **Commutative:** `merge(A, B) == merge(B, A)` — order doesn't matter.
- **Associative:** `merge(A, merge(B, C)) == merge(merge(A, B), C)` — grouping doesn't matter.
- **Idempotent:** `merge(A, A) == A` — applying the same update twice is safe.

These properties allow any two replicas to merge in any order and always converge to the same result.

### G-Counter (Grow-only Counter)

Each node maintains its own counter. The value is the sum of all node counters. Merging two G-Counters takes the max of each slot.

```
Node A: {A:5, B:3, C:2}  value = 10
Node B: {A:4, B:7, C:2}  value = 13
Merged: {A:5, B:7, C:2}  value = 14
```

---

## Hands-on Assignment (Go)

### Step 1: Set up the project

```bash
mkdir dist-sys-day29
cd dist-sys-day29
go mod init day29
```

### Step 2: Implement G-Counter

```go
package main

import (
	"fmt"
	"sync"
)

type GCounter struct {
	mu    sync.Mutex
	nodes map[string]int
	self  string
}

func NewGCounter(nodeID string) *GCounter {
	return &GCounter{
		nodes: map[string]int{nodeID: 0},
		self:  nodeID,
	}
}

func (g *GCounter) Increment() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nodes[g.self]++
}

func (g *GCounter) Value() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	total := 0
	for _, v := range g.nodes {
		total += v
	}
	return total
}

func (g *GCounter) Merge(other *GCounter) {
	g.mu.Lock()
	other.mu.Lock()
	defer g.mu.Unlock()
	defer other.mu.Unlock()
	for node, count := range other.nodes {
		if count > g.nodes[node] {
			g.nodes[node] = count
		}
	}
}

func (g *GCounter) Snapshot() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	snap := make(map[string]int)
	for k, v := range g.nodes {
		snap[k] = v
	}
	return snap
}

func main() {
	cA := NewGCounter("A")
	cB := NewGCounter("B")
	cC := NewGCounter("C")

	var wg sync.WaitGroup

	// Each goroutine increments independently (simulates concurrent writes)
	wg.Add(3)
	go func() { defer wg.Done(); for i := 0; i < 5; i++ { cA.Increment() } }()
	go func() { defer wg.Done(); for i := 0; i < 3; i++ { cB.Increment() } }()
	go func() { defer wg.Done(); for i := 0; i < 7; i++ { cC.Increment() } }()
	wg.Wait()

	fmt.Printf("Before merge:\n")
	fmt.Printf("  A: %v (value=%d)\n", cA.Snapshot(), cA.Value())
	fmt.Printf("  B: %v (value=%d)\n", cB.Snapshot(), cB.Value())
	fmt.Printf("  C: %v (value=%d)\n", cC.Snapshot(), cC.Value())

	// Merge in one order: A ← B ← C
	cA.Merge(cB)
	cA.Merge(cC)
	fmt.Printf("\nAfter merging into A: %v (value=%d)\n", cA.Snapshot(), cA.Value())

	// Merge in different order: B ← C ← A  (commutativity check)
	cB.Merge(cC)
	cB.Merge(cA)
	fmt.Printf("After merging into B: %v (value=%d)\n", cB.Snapshot(), cB.Value())

	// Both should give the same result — that is the CRDT guarantee
	if cA.Value() == cB.Value() {
		fmt.Println("\n✅ Commutativity verified: merge order does not affect result")
	}

	// Idempotency check: merge cA into itself
	before := cA.Value()
	cA.Merge(cA)
	if cA.Value() == before {
		fmt.Println("✅ Idempotency verified: merging with self changes nothing")
	}
}
```

### Step 3: Run it

```bash
go run main.go
```

Verify:
- Total value = 5 + 3 + 7 = 15 regardless of merge order.
- Merging again with the same data changes nothing.

### Step 4: LWW comparison

Add a simple LWW conflict demo:

```go
type LWWRegister struct {
	value string
	ts    int64
}

func (r *LWWRegister) Write(value string, ts int64) {
	if ts > r.ts {
		r.value = value
		r.ts = ts
	}
}

// Two concurrent writes:
reg := &LWWRegister{}
reg.Write("Kha", 100)
reg.Write("Bob", 99)
fmt.Printf("LWW result: %q (write with ts=99 was silently lost)\n", reg.value)
```

---

## Review

1. LWW silently loses data. In what real-world use cases is that acceptable?

2. A PN-Counter (Positive-Negative Counter) allows both increments and decrements. It is implemented as two G-Counters: one for increments, one for decrements. The value is `P.Value() - N.Value()`. Does this still satisfy the CRDT properties? Why?
