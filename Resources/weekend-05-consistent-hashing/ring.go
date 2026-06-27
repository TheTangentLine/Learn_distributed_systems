package main

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring is a consistent hash ring with virtual node support.
type Ring struct {
	mu      sync.RWMutex
	vnodes  int
	sorted  []uint32          // sorted hash positions
	hashMap map[uint32]string // position → physical node name
}

// NewRing creates a ring with the given number of virtual nodes per physical node.
// A good default is 150; lower values produce uneven distribution.
func NewRing(vnodes int) *Ring {
	return &Ring{
		vnodes:  vnodes,
		hashMap: make(map[uint32]string),
	}
}

func (r *Ring) hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// AddNode places `vnodes` virtual nodes for the physical node onto the ring.
func (r *Ring) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.vnodes; i++ {
		h := r.hash(node + "-" + strconv.Itoa(i))
		r.sorted = append(r.sorted, h)
		r.hashMap[h] = node
	}
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

// RemoveNode removes all virtual nodes for the physical node from the ring.
func (r *Ring) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.vnodes; i++ {
		h := r.hash(node + "-" + strconv.Itoa(i))
		delete(r.hashMap, h)
	}
	// Rebuild sorted slice without deleted positions
	r.sorted = r.sorted[:0]
	for k := range r.hashMap {
		r.sorted = append(r.sorted, k)
	}
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

// GetNode returns the physical node responsible for the given key.
// It walks clockwise from the key's hash position to the next virtual node.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sorted) == 0 {
		return ""
	}
	h := r.hash(key)
	idx := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= h })
	if idx == len(r.sorted) {
		idx = 0 // wrap around
	}
	return r.hashMap[r.sorted[idx]]
}

// GetN returns the N distinct physical nodes responsible for key,
// walking clockwise from the key's position. Used for replication.
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
