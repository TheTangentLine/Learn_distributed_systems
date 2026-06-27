# Day 41: Distributed KV Store — Implementation

## Overview

Today we build the full KV store in a single `main.go` file. Each node reads its own address and the peer list from environment variables, so you can run multiple instances locally.

---

## Hands-on Assignment (Go)

### Step 1: Create `main.go`

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Vector Clock ────────────────────────────────────────────────────────────

type VC map[string]int

func (vc VC) Inc(node string) VC {
	next := make(VC, len(vc)+1)
	for k, v := range vc { next[k] = v }
	next[node]++
	return next
}

// Merge takes the element-wise max
func (vc VC) Merge(other VC) VC {
	result := make(VC, len(vc))
	for k, v := range vc { result[k] = v }
	for k, v := range other {
		if v > result[k] { result[k] = v }
	}
	return result
}

// Compare: -1 a<b, 1 a>b, 0 concurrent
func (a VC) Compare(b VC) int {
	aLess, bLess := false, false
	keys := map[string]bool{}
	for k := range a { keys[k] = true }
	for k := range b { keys[k] = true }
	for k := range keys {
		if a[k] < b[k] { aLess = true }
		if a[k] > b[k] { bLess = true }
	}
	if aLess && !bLess { return -1 }
	if bLess && !aLess { return 1 }
	return 0
}

// ─── Consistent Hash Ring ─────────────────────────────────────────────────────

type Ring struct {
	mu      sync.RWMutex
	vnodes  int
	sorted  []uint32
	hashMap map[uint32]string
}

func NewRing(vnodes int) *Ring {
	return &Ring{vnodes: vnodes, hashMap: make(map[uint32]string)}
}

func (r *Ring) hash(s string) uint32 { return crc32.ChecksumIEEE([]byte(s)) }

func (r *Ring) Add(node string) {
	r.mu.Lock(); defer r.mu.Unlock()
	for i := 0; i < r.vnodes; i++ {
		h := r.hash(node + "-" + strconv.Itoa(i))
		r.sorted = append(r.sorted, h)
		r.hashMap[h] = node
	}
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

func (r *Ring) GetN(key string, n int) []string {
	r.mu.RLock(); defer r.mu.RUnlock()
	if len(r.sorted) == 0 { return nil }
	h := r.hash(key)
	idx := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= h })
	if idx == len(r.sorted) { idx = 0 }
	seen := map[string]bool{}
	result := []string{}
	for len(result) < n && len(seen) < len(r.hashMap)/r.vnodes {
		node := r.hashMap[r.sorted[idx%len(r.sorted)]]
		if !seen[node] { seen[node] = true; result = append(result, node) }
		idx++
	}
	return result
}

// ─── Local Store ──────────────────────────────────────────────────────────────

type Store struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

type Entry struct {
	Value  string `json:"value"`
	Clock  VC     `json:"clock"`
	Deleted bool  `json:"deleted,omitempty"`
}

func NewStore() *Store { return &Store{data: make(map[string]*Entry)} }

func (s *Store) Set(key string, e *Entry) {
	s.mu.Lock(); defer s.mu.Unlock()
	s.data[key] = e
}

func (s *Store) Get(key string) (*Entry, bool) {
	s.mu.RLock(); defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}

// ─── Globals ──────────────────────────────────────────────────────────────────

var (
	selfAddr string
	ring     *Ring
	store    = NewStore()
	N        = 3
	W        = 2
	R        = 2
)

// ─── Coordinator ──────────────────────────────────────────────────────────────

func coordinatorPut(key, value string) (*Entry, error) {
	var existing VC
	if e, ok := store.Get(key); ok { existing = e.Clock }
	newVC := existing.Inc(selfAddr)
	entry := &Entry{Value: value, Clock: newVC}

	nodes := ring.GetN(key, N)
	type result struct{ ok bool }
	results := make(chan result, len(nodes))

	for _, node := range nodes {
		go func(addr string) {
			body, _ := json.Marshal(entry)
			url := fmt.Sprintf("http://%s/internal/set/%s", addr, key)
			resp, err := http.Post(url, "application/json", bytes.NewReader(body))
			if err == nil && resp.StatusCode == 200 {
				results <- result{true}
			} else {
				results <- result{false}
			}
		}(node)
	}

	acks := 0
	timeout := time.After(500 * time.Millisecond)
	for i := 0; i < len(nodes); i++ {
		select {
		case r := <-results:
			if r.ok { acks++ }
		case <-timeout:
		}
	}

	if acks < W {
		return nil, fmt.Errorf("quorum not reached: %d/%d acks", acks, W)
	}
	return entry, nil
}

func coordinatorGet(key string) (*Entry, error) {
	nodes := ring.GetN(key, N)
	type result struct {
		entry *Entry
		addr  string
	}
	results := make(chan result, len(nodes))

	for _, node := range nodes {
		go func(addr string) {
			url := fmt.Sprintf("http://%s/internal/get/%s", addr, key)
			resp, err := http.Get(url)
			if err != nil || resp.StatusCode != 200 {
				results <- result{}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			var e Entry
			if json.Unmarshal(body, &e) == nil {
				results <- result{&e, addr}
			} else {
				results <- result{}
			}
		}(node)
	}

	var best *Entry
	var bestAddr string
	responses := 0
	timeout := time.After(500 * time.Millisecond)
	for i := 0; i < len(nodes); i++ {
		select {
		case r := <-results:
			if r.entry == nil { continue }
			responses++
			if best == nil || r.entry.Clock.Compare(best.Clock) > 0 {
				best = r.entry
				bestAddr = r.addr
			}
		case <-timeout:
		}
	}
	_ = bestAddr

	if responses < R { return nil, fmt.Errorf("read quorum not reached") }
	if best == nil { return nil, fmt.Errorf("key not found") }
	return best, nil
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

func putHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/key/")
	var req struct{ Value string `json:"value"` }
	json.NewDecoder(r.Body).Decode(&req)

	entry, err := coordinatorPut(key, req.Value)
	if err != nil { http.Error(w, err.Error(), 500); return }
	json.NewEncoder(w).Encode(entry)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/key/")
	entry, err := coordinatorGet(key)
	if err != nil { http.Error(w, err.Error(), 404); return }
	json.NewEncoder(w).Encode(entry)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/key/")
	entry := &Entry{Value: "", Clock: VC{selfAddr: 1}, Deleted: true}
	store.Set(key, entry)
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func internalSet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/set/")
	var e Entry
	json.NewDecoder(r.Body).Decode(&e)
	store.Set(key, &e)
	fmt.Fprintf(w, "ok")
}

func internalGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/get/")
	e, ok := store.Get(key)
	if !ok { http.NotFound(w, r); return }
	json.NewEncoder(w).Encode(e)
}

func main() {
	selfAddr = os.Getenv("NODE_ADDR")
	if selfAddr == "" { selfAddr = "localhost:8081" }

	nodeList := strings.Split(os.Getenv("NODE_LIST"), ",")
	ring = NewRing(150)
	for _, n := range nodeList {
		if n != "" { ring.Add(strings.TrimSpace(n)) }
	}

	http.HandleFunc("/key/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:  putHandler(w, r)
		case http.MethodGet:  getHandler(w, r)
		case http.MethodDelete: deleteHandler(w, r)
		default: http.Error(w, "method not allowed", 405)
		}
	})
	http.HandleFunc("/internal/set/", internalSet)
	http.HandleFunc("/internal/get/", internalGet)

	var started int32
	go func() {
		time.Sleep(100 * time.Millisecond)
		atomic.StoreInt32(&started, 1)
	}()

	log.Printf("KV node %s starting (ring nodes: %v)", selfAddr, nodeList)
	log.Fatal(http.ListenAndServe(selfAddr, nil))
}
```

### Step 2: Run three nodes

```bash
# Terminal 1
NODE_ADDR=localhost:8081 NODE_LIST=localhost:8081,localhost:8082,localhost:8083 go run main.go

# Terminal 2
NODE_ADDR=localhost:8082 NODE_LIST=localhost:8081,localhost:8082,localhost:8083 go run main.go

# Terminal 3
NODE_ADDR=localhost:8083 NODE_LIST=localhost:8081,localhost:8082,localhost:8083 go run main.go
```

### Step 3: Test the store

```bash
# PUT via any node
curl -X PUT localhost:8081/key/name -d '{"value":"Kha"}' -H "Content-Type: application/json"

# GET via a different node (inter-node routing)
curl localhost:8082/key/name
curl localhost:8083/key/name

# Verify all nodes converge
```

---

## Review

Does the current implementation perform read repair? If two replicas return different values (different vector clocks), what does `coordinatorGet` currently do? How would you add read repair?
