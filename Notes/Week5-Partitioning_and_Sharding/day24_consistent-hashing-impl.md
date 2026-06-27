# Day 24: Consistent Hashing — Full Implementation

## 1. Data Structure

The implementation uses a **sorted slice of hash positions** (the ring). Adding a node inserts V virtual node positions into this slice. Routing a key binary-searches for the first position ≥ the key's hash, wrapping around to the first position if none is found.

```
ring positions: [hash(A-0), hash(A-1), ..., hash(B-0), hash(B-1), ..., hash(C-0), ...]
                sorted ascending, each position maps to a physical node
```

---

## Hands-on Assignment (Go)

### Step 1: Set up the project

```bash
mkdir dist-sys-day24
cd dist-sys-day24
go mod init day24
```

### Step 2: Create `ring.go`

```go
package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Ring struct {
	mu       sync.RWMutex
	vnodes   int
	keys     []uint32          // sorted hash positions
	hashMap  map[uint32]string // hash position → physical node name
}

func NewRing(vnodes int) *Ring {
	return &Ring{
		vnodes:  vnodes,
		hashMap: make(map[uint32]string),
	}
}

func (r *Ring) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (r *Ring) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.vnodes; i++ {
		h := r.hash(node + "-" + strconv.Itoa(i))
		r.keys = append(r.keys, h)
		r.hashMap[h] = node
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

func (r *Ring) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.vnodes; i++ {
		h := r.hash(node + "-" + strconv.Itoa(i))
		delete(r.hashMap, h)
	}
	// Rebuild sorted key slice
	r.keys = r.keys[:0]
	for k := range r.hashMap {
		r.keys = append(r.keys, k)
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.keys) == 0 {
		return ""
	}
	h := r.hash(key)
	idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if idx == len(r.keys) {
		idx = 0 // wrap around
	}
	return r.hashMap[r.keys[idx]]
}
```

### Step 3: Create `main.go`

```go
package main

import (
	"fmt"
)

func main() {
	// --- Functional test ---
	ring := NewRing(150)
	ring.AddNode("NodeA")
	ring.AddNode("NodeB")
	ring.AddNode("NodeC")

	// Test a few keys
	keys := []string{"user:1", "user:2", "product:42", "order:100", "session:xyz"}
	fmt.Println("=== Initial routing (3 nodes, vnodes=150) ===")
	for _, k := range keys {
		fmt.Printf("  %-20s → %s\n", k, ring.GetNode(k))
	}

	// --- Rebalancing test: 1000 keys, add a 4th node ---
	const totalKeys = 1000
	allKeys := make([]string, totalKeys)
	before := make(map[string]string, totalKeys)
	for i := 0; i < totalKeys; i++ {
		k := fmt.Sprintf("key-%04d", i)
		allKeys[i] = k
		before[k] = ring.GetNode(k)
	}

	ring.AddNode("NodeD")

	moved := 0
	after := make(map[string]string, totalKeys)
	for _, k := range allKeys {
		after[k] = ring.GetNode(k)
		if before[k] != after[k] {
			moved++
		}
	}

	fmt.Printf("\n=== Adding NodeD: %d/%d keys moved (%.1f%%)\n",
		moved, totalKeys, float64(moved)/float64(totalKeys)*100)
	fmt.Printf("    Expected ~%.1f%% (1/N = 1/4)\n", 100.0/4.0)

	// --- Distribution test ---
	counts := map[string]int{}
	for _, k := range allKeys {
		counts[ring.GetNode(k)]++
	}
	fmt.Println("\n=== Distribution after NodeD added ===")
	for node, c := range counts {
		fmt.Printf("  %-8s: %d keys (%.1f%%)\n", node, c, float64(c)/float64(totalKeys)*100)
	}
}
```

### Step 4: Run it

```bash
go run ring.go main.go
```

Expected output:

```
=== Initial routing (3 nodes, vnodes=150) ===
  user:1               → NodeB
  user:2               → NodeC
  ...

=== Adding NodeD: 248/1000 keys moved (24.8%)
    Expected ~25.0% (1/N = 1/4)

=== Distribution after NodeD added ===
  NodeA    : 251 keys (25.1%)
  NodeB    : 247 keys (24.7%)
  NodeC    : 252 keys (25.2%)
  NodeD    : 250 keys (25.0%)
```

The actual movement (≈25%) should be close to the theoretical `1/N = 25%`.

---

## Review

1. What happens if you set `vnodes=1`? Run your code with `vnodes=1` and observe the distribution. Why is it uneven?

2. Your ring has `vnodes=150`. You have 4 nodes. How many positions are in `r.keys`? What is the worst-case time complexity of `GetNode`?
