# Day 21: Why Partition?

## 1. The Single-Node Ceiling

A single machine is fast, simple, and easy to reason about. But it has hard ceilings:

| Resource | Realistic maximum | Why it matters |
|----------|-------------------|---------------|
| RAM | ~4 TB (high-end server) | In-memory indexes, caches |
| Disk IOPS | ~1M (NVMe) | Random reads under load |
| Network bandwidth | ~100 Gbps | Read/write throughput |
| CPU cores | ~256 | Query parallelism |

When your dataset or traffic exceeds one machine, you have two choices.

## 2. Vertical vs Horizontal Scaling

**Vertical scaling (scale up):** buy a bigger machine.
- Simple — no code changes.
- Expensive — enterprise hardware costs 10× more for 2× the performance.
- Single point of failure — the biggest machine is still one machine.
- Hard ceiling — you cannot scale beyond the biggest available server.

**Horizontal scaling (scale out):** add more commodity machines and split the data.
- Cheap at scale — 10 commodity servers cost less than one enterprise server.
- No hard ceiling — add machines as traffic grows.
- Fault tolerant — one machine dying does not take down the system.
- Complex — now you have the problems covered in Weeks 2–4.

Partitioning is the mechanism that makes horizontal scaling possible for **data**.

---

## Hands-on Assignment (Go)

We measure the concrete benefit of parallelism — the simplest form of partitioning.

### Step 1: Set up the project

```bash
mkdir dist-sys-day21
cd dist-sys-day21
go mod init day21
```

### Step 2: Create `bench.go`

```go
package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

const itemCount = 1_000_000

func buildData() []int {
	data := make([]int, itemCount)
	for i := range data {
		data[i] = i
	}
	return data
}

// Sequential scan: one goroutine processes all items
func sequentialSum(data []int) int64 {
	var sum int64
	for _, v := range data {
		sum += int64(v)
	}
	return sum
}

// Parallel scan: N goroutines each process 1/N of the data
func parallelSum(data []int, workers int) int64 {
	chunkSize := len(data) / workers
	results := make([]int64, workers)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		start := w * chunkSize
		end := start + chunkSize
		if w == workers-1 {
			end = len(data)
		}
		go func(id, s, e int) {
			defer wg.Done()
			var sum int64
			for _, v := range data[s:e] {
				sum += int64(v)
			}
			results[id] = sum
		}(w, start, end)
	}

	wg.Wait()
	var total int64
	for _, r := range results {
		total += r
	}
	return total
}

func bench(label string, fn func() int64) {
	start := time.Now()
	result := fn()
	elapsed := time.Since(start)
	fmt.Printf("%-30s result=%d  time=%v\n", label, result, elapsed)
}

func main() {
	data := buildData()
	cores := runtime.NumCPU()
	fmt.Printf("CPU cores available: %d\n\n", cores)

	bench("Sequential (1 goroutine):", func() int64 { return sequentialSum(data) })
	bench("Parallel (2 goroutines):", func() int64 { return parallelSum(data, 2) })
	bench("Parallel (4 goroutines):", func() int64 { return parallelSum(data, 4) })
	bench(fmt.Sprintf("Parallel (%d goroutines):", cores),
		func() int64 { return parallelSum(data, cores) })
}
```

### Step 3: Run the benchmark

```bash
go run bench.go
```

Example output (on a 4-core machine):

```
CPU cores available: 4

Sequential (1 goroutine):      result=499999500000  time=1.2ms
Parallel (2 goroutines):       result=499999500000  time=0.7ms
Parallel (4 goroutines):       result=499999500000  time=0.4ms
Parallel (4 goroutines):       result=499999500000  time=0.4ms
```

### Step 4: Extrapolate

The parallel version is ~3× faster on 4 cores. If your dataset is 1 TB and you partition it across 10 nodes, a scan that would take 10 minutes on 1 node can finish in ~1 minute on 10 nodes — because all 10 scan their slice simultaneously.

This is exactly how **distributed database scans** (Cassandra, BigQuery, Spark) work.

---

## Review

1. Your PostgreSQL table has 500 million rows and a full table scan takes 3 minutes. You shard it across 5 nodes. Assuming perfect distribution and no coordination overhead, how long does the scan take?

2. Vertical scaling vs horizontal scaling — name one real-world scenario where vertical scaling is the **right** choice despite its limitations.
