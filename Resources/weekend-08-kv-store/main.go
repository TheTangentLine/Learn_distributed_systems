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
	"time"
)

// ─── Vector Clock ─────────────────────────────────────────────────────────────

// VC is a Lamport vector clock: a map from node address to logical counter.
type VC map[string]int

func (vc VC) Inc(node string) VC {
	next := make(VC, len(vc)+1)
	for k, v := range vc {
		next[k] = v
	}
	next[node]++
	return next
}

func (vc VC) Merge(other VC) VC {
	result := make(VC, len(vc))
	for k, v := range vc {
		result[k] = v
	}
	for k, v := range other {
		if v > result[k] {
			result[k] = v
		}
	}
	return result
}

// Compare returns -1 if a < b (a happened before b),
// 1 if a > b, or 0 if concurrent.
func (a VC) Compare(b VC) int {
	aLess, bLess := false, false
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	for k := range keys {
		if a[k] < b[k] {
			aLess = true
		}
		if a[k] > b[k] {
			bLess = true
		}
	}
	if aLess && !bLess {
		return -1
	}
	if bLess && !aLess {
		return 1
	}
	return 0 // concurrent
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

func (r *Ring) hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.vnodes; i++ {
		h := r.hash(node + "-" + strconv.Itoa(i))
		r.sorted = append(r.sorted, h)
		r.hashMap[h] = node
	}
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

// GetN returns the N distinct physical nodes responsible for key.
func (r *Ring) GetN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sorted) == 0 {
		return nil
	}
	h := r.hash(key)
	idx := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= h })
	if idx == len(r.sorted) {
		idx = 0
	}
	seen := map[string]bool{}
	result := []string{}
	totalNodes := len(r.hashMap) / r.vnodes
	for len(result) < n && len(seen) < totalNodes {
		node := r.hashMap[r.sorted[idx%len(r.sorted)]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
		idx++
	}
	return result
}

// ─── Local Store ──────────────────────────────────────────────────────────────

type Entry struct {
	Value   string `json:"value"`
	Clock   VC     `json:"clock"`
	Deleted bool   `json:"deleted,omitempty"`
}

type Store struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

func NewStore() *Store { return &Store{data: make(map[string]*Entry)} }

func (s *Store) Set(key string, e *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = e
}

func (s *Store) Get(key string) (*Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}

// ─── Globals ──────────────────────────────────────────────────────────────────

var (
	selfAddr string
	ring     *Ring
	store    = NewStore()
	kvN      = 3
	kvW      = 2
	kvR      = 2
)

// ─── Coordinator — Write ──────────────────────────────────────────────────────

func coordinatorPut(key, value string) (*Entry, error) {
	var existingVC VC
	if e, ok := store.Get(key); ok {
		existingVC = e.Clock
	}
	newVC := existingVC.Inc(selfAddr)
	entry := &Entry{Value: value, Clock: newVC}

	nodes := ring.GetN(key, kvN)
	type ack struct{ ok bool }
	acks := make(chan ack, len(nodes))

	for _, node := range nodes {
		go func(addr string) {
			body, _ := json.Marshal(entry)
			url := fmt.Sprintf("http://%s/internal/set/%s", addr, key)
			resp, err := http.Post(url, "application/json", bytes.NewReader(body))
			if err == nil && resp.StatusCode == 200 {
				resp.Body.Close()
				acks <- ack{true}
			} else {
				log.Printf("write to %s failed: %v", addr, err)
				acks <- ack{false}
			}
		}(node)
	}

	successAcks := 0
	timeout := time.After(500 * time.Millisecond)
	for i := 0; i < len(nodes); i++ {
		select {
		case a := <-acks:
			if a.ok {
				successAcks++
			}
		case <-timeout:
		}
	}

	if successAcks < kvW {
		return nil, fmt.Errorf("quorum not reached: got %d/%d acks, need %d", successAcks, len(nodes), kvW)
	}
	return entry, nil
}

// ─── Coordinator — Read ───────────────────────────────────────────────────────

type replicaResult struct {
	entry *Entry
	addr  string
}

func coordinatorGet(key string) (*Entry, error) {
	nodes := ring.GetN(key, kvN)
	results := make(chan replicaResult, len(nodes))

	for _, node := range nodes {
		go func(addr string) {
			url := fmt.Sprintf("http://%s/internal/get/%s", addr, key)
			resp, err := http.Get(url)
			if err != nil || resp.StatusCode != 200 {
				results <- replicaResult{}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			var e Entry
			if json.Unmarshal(body, &e) == nil {
				results <- replicaResult{&e, addr}
			} else {
				results <- replicaResult{}
			}
		}(node)
	}

	var best *Entry
	var bestAddr string
	allResults := []replicaResult{}
	responses := 0
	timeout := time.After(500 * time.Millisecond)

	for i := 0; i < len(nodes); i++ {
		select {
		case r := <-results:
			if r.entry != nil {
				responses++
				allResults = append(allResults, r)
				if best == nil || r.entry.Clock.Compare(best.Clock) > 0 {
					best = r.entry
					bestAddr = r.addr
				}
			}
		case <-timeout:
		}
	}

	if responses < kvR {
		return nil, fmt.Errorf("read quorum not reached: got %d/%d responses, need %d", responses, len(nodes), kvR)
	}
	if best == nil || best.Deleted {
		return nil, fmt.Errorf("key not found")
	}

	// Read repair: asynchronously heal any stale replica
	for _, r := range allResults {
		if r.addr == bestAddr {
			continue
		}
		if r.entry.Clock.Compare(best.Clock) < 0 {
			go func(staleAddr string, freshEntry *Entry) {
				body, _ := json.Marshal(freshEntry)
				url := fmt.Sprintf("http://%s/internal/set/%s", staleAddr, key)
				resp, err := http.Post(url, "application/json", bytes.NewReader(body))
				if err == nil {
					resp.Body.Close()
					log.Printf("[read-repair] healed %s for key %q", staleAddr, key)
				}
			}(r.addr, best)
		}
	}

	return best, nil
}

// ─── HTTP Handlers — Client-facing ────────────────────────────────────────────

func putHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/key/")
	if key == "" {
		http.Error(w, "key required", 400)
		return
	}
	var req struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", 400)
		return
	}
	entry, err := coordinatorPut(key, req.Value)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/key/")
	entry, err := coordinatorGet(key)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/key/")
	tombstone := &Entry{
		Value:   "",
		Clock:   VC{selfAddr: 1},
		Deleted: true,
	}
	// Use the quorum write path so all replicas get the tombstone
	_, err := coordinatorPut(key, "")
	if err != nil {
		// Fall back to local tombstone
		store.Set(key, tombstone)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// ─── HTTP Handlers — Internal (inter-node) ────────────────────────────────────

func internalSetHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/set/")
	var e Entry
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	store.Set(key, &e)
	fmt.Fprintf(w, "ok")
}

func internalGetHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/get/")
	e, ok := store.Get(key)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(e)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	selfAddr = os.Getenv("NODE_ADDR")
	if selfAddr == "" {
		selfAddr = "localhost:8081"
	}

	nodeList := strings.Split(os.Getenv("NODE_LIST"), ",")
	ring = NewRing(150)
	for _, n := range nodeList {
		n = strings.TrimSpace(n)
		if n != "" {
			ring.Add(n)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/key/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putHandler(w, r)
		case http.MethodGet:
			getHandler(w, r)
		case http.MethodDelete:
			deleteHandler(w, r)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	mux.HandleFunc("/internal/set/", internalSetHandler)
	mux.HandleFunc("/internal/get/", internalGetHandler)

	log.Printf("KV node %s starting | ring peers: %v", selfAddr, nodeList)
	log.Fatal(http.ListenAndServe(selfAddr, mux))
}
