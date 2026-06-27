# Day 22: Partitioning Strategies

## 1. The Core Question

Given a key, which node should store it? There are two fundamental strategies.

## 2. Key-Range Partitioning

Assign each node a **contiguous range** of keys. The key space is sorted and divided into ranges.

- **Advantage:** range queries are fast — `GET users WHERE id BETWEEN 1000 AND 2000` touches only one partition.
- **Disadvantage:** sequential keys create hot spots. If your keys are timestamps and you write time-series data, every write goes to the partition holding the latest range.

**Used by:** HBase, Google Bigtable, Apache Cassandra (with clustering keys).

## 3. Hash Partitioning

Hash the key to a number, then assign it to a node using modulo:

```
node = hash(key) % N
```

- **Advantage:** uniform distribution — sequential keys get spread across all nodes.
- **Disadvantage:** range queries require scanning all N nodes. Adding or removing one node reassigns `N-1/N` of all keys (almost everything moves).

**Used by:** Redis Cluster (hash slots 0–16383), Elasticsearch.

### The rebalancing problem

With `N=3` nodes and `hash(key) % 3`:

| Key | hash | node (N=3) | node (N=4) — after adding one node |
|-----|------|------------|--------------------------------------|
| "a" | 97 | 97%3 = 1 | 97%4 = 1 ✓ |
| "b" | 98 | 98%3 = 2 | 98%4 = 2 ✓ |
| "c" | 99 | 99%3 = 0 | 99%4 = 3 ✗ moved! |
| "d" | 100 | 100%3 = 1 | 100%4 = 0 ✗ moved! |

Almost every key moves when N changes. **Consistent hashing** (Day 23) solves this.

---

## Hands-on Assignment (Go)

We implement both routing functions and compare the distribution of 10,000 sequential keys.

### Step 1: Create `partition.go`

```go
package main

import (
	"fmt"
	"hash/fnv"
)

const numNodes = 3

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// Key-range partition: lexicographic split into thirds
func keyRangeNode(key string) int {
	if key < "d" {
		return 0
	} else if key < "r" {
		return 1
	}
	return 2
}

// Hash partition
func hashNode(key string) int {
	return int(hashKey(key)) % numNodes
}

func distribution(label string, fn func(string) int, keys []string) {
	counts := make([]int, numNodes)
	for _, k := range keys {
		counts[fn(k)]++
	}
	fmt.Printf("%s:\n", label)
	for i, c := range counts {
		pct := float64(c) / float64(len(keys)) * 100
		fmt.Printf("  Node %d: %5d keys (%5.1f%%)\n", i, c, pct)
	}
	fmt.Println()
}

func main() {
	// Generate 10,000 sequential keys: "key-00000" to "key-09999"
	keys := make([]string, 10_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%05d", i)
	}

	distribution("Key-range partition", keyRangeNode, keys)
	distribution("Hash partition", hashNode, keys)

	// Now generate time-series keys
	timeSeries := make([]string, 1000)
	for i := range timeSeries {
		timeSeries[i] = fmt.Sprintf("2024-01-01T00:%02d:%02d", i/60, i%60)
	}

	fmt.Println("--- Time-series keys (shows key-range hot spot) ---")
	distribution("Key-range (time-series)", keyRangeNode, timeSeries)
	distribution("Hash (time-series)", hashNode, timeSeries)
}
```

### Step 2: Run it

```bash
go run partition.go
```

Observe:
- **Key-range** with sequential keys like `key-00000` to `key-09999` concentrates them on fewer nodes.
- **Hash** distributes nearly uniformly (~33% each for 3 nodes).
- With time-series keys, key-range puts everything on Node 2 (the "latest" range) — the classic hot spot.

---

## Review

1. You are building a social media feed. Posts are keyed by `userID:timestamp`. You want to quickly fetch all posts from a user sorted by time. Should you use key-range or hash partitioning? Why?

2. You are building a URL shortener. Short codes are random 6-character strings. Which strategy prevents hot spots?
