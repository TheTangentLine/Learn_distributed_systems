# Day 13: Vector Clocks

## 1. What Lamport Clocks Cannot Tell You

Lamport clocks give you a partial order: if `a → b` then `LC(a) < LC(b)`. But the converse is not true — a lower timestamp does not prove one event caused another. Two events can have `LC(a) < LC(b)` yet be entirely unrelated (concurrent).

**Vector clocks** fix this. They capture the full causal history of every event, letting you determine whether any two events are ordered or concurrent.

## 2. The Rules

Each node `i` maintains an array of N integers — one per node. `VC[i]` is node `i`'s view of its own logical time.

1. **Internal event at node i:** `VC[i]++`
2. **Send from node i:** increment `VC[i]`, include full `VC` in the message.
3. **Receive at node j of message with `VC_msg`:** for each k, `VC[k] = max(VC[k], VC_msg[k])`, then `VC[j]++`.

## 3. Comparing Vector Clocks

Given `VC_a` and `VC_b`:

- `VC_a < VC_b` (a happened before b): `VC_a[k] ≤ VC_b[k]` for all k, with strict `<` for at least one k.
- `VC_a > VC_b`: the reverse.
- `VC_a ∥ VC_b` (**concurrent**): neither dominates — they have diverged. This is a **write conflict**.

---

## Hands-on Assignment (Go)

### Step 1: Set up the project

```bash
mkdir dist-sys-day13
cd dist-sys-day13
go mod init day13
```

### Step 2: Create `vector_clock.go`

```go
package main

import (
	"fmt"
	"sync"
)

type VectorClock struct {
	mu    sync.Mutex
	clock map[string]int
	self  string
}

func NewVC(nodeID string) *VectorClock {
	return &VectorClock{
		clock: map[string]int{nodeID: 0},
		self:  nodeID,
	}
}

func (vc *VectorClock) Tick() map[string]int {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clock[vc.self]++
	return vc.snapshot()
}

func (vc *VectorClock) Merge(incoming map[string]int) map[string]int {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	for k, v := range incoming {
		if v > vc.clock[k] {
			vc.clock[k] = v
		}
	}
	vc.clock[vc.self]++
	return vc.snapshot()
}

func (vc *VectorClock) snapshot() map[string]int {
	snap := make(map[string]int, len(vc.clock))
	for k, v := range vc.clock {
		snap[k] = v
	}
	return snap
}

// Returns: -1 (a < b), 1 (a > b), 0 (concurrent)
func Compare(a, b map[string]int) int {
	aLessB, bLessA := false, false
	keys := make(map[string]bool)
	for k := range a { keys[k] = true }
	for k := range b { keys[k] = true }

	for k := range keys {
		if a[k] < b[k] { aLessB = true }
		if a[k] > b[k] { bLessA = true }
	}
	if aLessB && !bLessA { return -1 }
	if bLessA && !aLessB { return 1 }
	return 0 // concurrent
}

type Message struct {
	from string
	body string
	vc   map[string]int
}

func main() {
	vcA := NewVC("A")
	vcB := NewVC("B")

	// A does an internal event
	snapA1 := vcA.Tick()
	fmt.Printf("A internal:  VC=%v\n", snapA1)

	// A sends to B
	snapA2 := vcA.Tick()
	msgAtoB := Message{from: "A", body: "write x=5", vc: snapA2}
	fmt.Printf("A sends:     VC=%v\n", snapA2)

	// B does an internal event BEFORE receiving from A
	snapB1 := vcB.Tick()
	fmt.Printf("B internal:  VC=%v\n", snapB1)

	// B receives from A
	snapB2 := vcB.Merge(msgAtoB.vc)
	fmt.Printf("B receives:  VC=%v\n", snapB2)

	// Now simulate a write conflict:
	// A and B both write to the same key without seeing each other's write
	vcA2 := NewVC("A")
	vcB2 := NewVC("B")

	writeA := vcA2.Tick() // A writes at VC={A:1}
	writeB := vcB2.Tick() // B writes at VC={B:1} — B did NOT see A's write

	fmt.Printf("\nConflict detection:\n")
	fmt.Printf("Write A: VC=%v\n", writeA)
	fmt.Printf("Write B: VC=%v\n", writeB)
	switch Compare(writeA, writeB) {
	case -1: fmt.Println("Result: A happened before B")
	case 1:  fmt.Println("Result: B happened before A")
	case 0:  fmt.Println("Result: CONCURRENT — write conflict detected!")
	}
}
```

### Step 3: Run it

```bash
go run vector_clock.go
```

Verify that in the conflict scenario, `Compare` returns `0` (concurrent) because neither `{A:1, B:0}` nor `{A:0, B:1}` dominates the other.

### Step 4: Resolve the conflict

Modify the experiment so B first receives A's write, then writes its own value. Run `Compare` again. Now it should return `1` (B happened after A).

---

## Review

1. A system has 3 nodes. Node C receives a message with `VC={A:3, B:2, C:0}` when C's current clock is `{A:2, B:1, C:4}`. What is C's clock after merging?

2. Vector clocks have a storage cost of N integers per message (N = number of nodes). For a cluster of 1000 nodes, what is the per-message overhead in bytes? How do systems like Riak work around this?
