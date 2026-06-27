# Day 34: Bloom Filters

## 1. The Problem

You have 10 billion URLs in a crawl cache. Before fetching a URL, you want to know: "have we already crawled this?"

Options:
- **Hash set in memory:** 10 billion 100-byte URLs = 1 TB RAM. Impossible.
- **Database lookup:** fast but adds latency to every URL check.
- **Bloom filter:** a few hundred MB of RAM, sub-microsecond checks, with a small chance of false positives.

## 2. How It Works

A Bloom filter is a **bit array** of `m` bits, all initially 0. It uses `k` independent hash functions.

**Insert `x`:** compute `h1(x), h2(x), ..., hk(x)`. Set those bit positions to 1.

**Query `x`:** compute the same positions. If **all** are 1 → "probably present". If **any** is 0 → "definitely absent".

**Key properties:**
- **No false negatives:** if you inserted something, a query always returns "present."
- **False positives possible:** by coincidence, all k bits for a new item might already be 1. The false positive rate is controlled by tuning m (bits) and k (hash functions).
- **No deletion:** setting a bit back to 0 would affect other items. (Counting Bloom filters solve this.)

### False positive rate formula

```
p ≈ (1 - e^(-kn/m))^k
```

Where n = number of items inserted. For p < 1%, with n=1 million items, you need ~10 bits per item = ~10 MB. Much less than a hash set.

---

## Hands-on Assignment (Go)

### Step 1: Set up the project

```bash
mkdir dist-sys-day34
cd dist-sys-day34
go mod init day34
```

### Step 2: Create `bloom.go`

```go
package main

import (
	"fmt"
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits []bool
	m    uint // number of bits
	k    uint // number of hash functions
}

func NewBloomFilter(m, k uint) *BloomFilter {
	return &BloomFilter{bits: make([]bool, m), m: m, k: k}
}

func (bf *BloomFilter) positions(key string) []uint {
	pos := make([]uint, bf.k)
	for i := uint(0); i < bf.k; i++ {
		h := fnv.New64a()
		fmt.Fprintf(h, "%s:%d", key, i) // different seed per hash function
		pos[i] = uint(h.Sum64()) % bf.m
	}
	return pos
}

func (bf *BloomFilter) Add(key string) {
	for _, p := range bf.positions(key) {
		bf.bits[p] = true
	}
}

func (bf *BloomFilter) MightContain(key string) bool {
	for _, p := range bf.positions(key) {
		if !bf.bits[p] {
			return false
		}
	}
	return true
}

// Optimal k for given m and n
func OptimalK(m, n uint) uint {
	k := float64(m) / float64(n) * math.Log(2)
	return uint(math.Round(k))
}

// Expected false positive rate
func FalsePositiveRate(m, k, n uint) float64 {
	inner := 1 - math.Exp(-float64(k)*float64(n)/float64(m))
	return math.Pow(inner, float64(k))
}
```

### Step 3: Create `main.go`

```go
package main

import (
	"fmt"
)

func main() {
	const insertCount = 1000
	const testCount = 200
	
	// Try different bit sizes, keep k=7
	for _, bitsPerItem := range []uint{8, 10, 16, 20} {
		m := bitsPerItem * insertCount
		k := OptimalK(m, insertCount)
		if k < 1 { k = 1 }

		bf := NewBloomFilter(m, k)

		// Insert 1000 URLs
		for i := 0; i < insertCount; i++ {
			bf.Add(fmt.Sprintf("https://example.com/page/%d", i))
		}

		// Test for false positives using 200 URLs that were NOT inserted
		falsePositives := 0
		for i := insertCount; i < insertCount+testCount; i++ {
			url := fmt.Sprintf("https://example.com/page/%d", i)
			if bf.MightContain(url) {
				falsePositives++
			}
		}

		fpRate := float64(falsePositives) / float64(testCount) * 100
		expected := FalsePositiveRate(m, k, insertCount) * 100

		fmt.Printf("m=%5d bits (%d/item), k=%d | FP rate: %.1f%% (expected: %.1f%%)\n",
			m, bitsPerItem, k, fpRate, expected)

		// Verify no false negatives
		for i := 0; i < insertCount; i++ {
			url := fmt.Sprintf("https://example.com/page/%d", i)
			if !bf.MightContain(url) {
				fmt.Println("ERROR: false negative detected!")
			}
		}
	}
	fmt.Println("\nNo false negatives detected ✅")
}
```

### Step 4: Run it

```bash
go run bloom.go main.go
```

Expected output:

```
m= 8000 bits (8/item), k=6  | FP rate: 2.0% (expected: 2.2%)
m=10000 bits (10/item), k=7 | FP rate: 1.0% (expected: 0.9%)
m=16000 bits (16/item), k=11| FP rate: 0.0% (expected: 0.1%)
m=20000 bits (20/item), k=14| FP rate: 0.0% (expected: 0.0%)

No false negatives detected ✅
```

---

## Review

1. A Bloom filter uses 10 bits per item with 7 hash functions. You have 10 million items. How much RAM does the filter require in megabytes? Compare to a naive hash set of 100-byte string keys.

2. Why can't you delete items from a basic Bloom filter? What data structure would you use if you need both insert and delete?
