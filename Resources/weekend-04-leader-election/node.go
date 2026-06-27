package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	pingInterval     = 150 * time.Millisecond
	watchdogInterval = 300 * time.Millisecond
	failureThreshold = 600 * time.Millisecond
)

type Status struct {
	NodeID   string `json:"node_id"`
	IsLeader bool   `json:"is_leader"`
	Term     int    `json:"term"`
}

type Node struct {
	mu       sync.Mutex
	id       string
	peers    []string
	isLeader bool
	term     int
	lastPing time.Time
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isLeader = true
	n.term++
	fmt.Printf("[%s] Became LEADER for term %d\n", n.id, n.term)
}

// handlePing is called when this node receives a heartbeat from the leader.
func (n *Node) handlePing(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	n.lastPing = time.Now()
	// If we thought we were leader but someone else is pinging us, step down.
	if n.isLeader {
		fmt.Printf("[%s] Stepping down — received ping from another leader\n", n.id)
		n.isLeader = false
	}
	n.mu.Unlock()
	fmt.Fprintf(w, "ack")
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	s := Status{NodeID: n.id, IsLeader: n.isLeader, Term: n.term}
	n.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s)
}

// sendPings broadcasts heartbeats to all peers while this node is leader.
func (n *Node) sendPings() {
	for {
		time.Sleep(pingInterval)
		n.mu.Lock()
		isLeader := n.isLeader
		n.mu.Unlock()
		if !isLeader {
			continue
		}
		for _, peer := range n.peers {
			go func(p string) {
				resp, err := http.Get("http://" + p + "/ping")
				if err != nil {
					fmt.Printf("[%s] ping to %s failed: %v\n", n.id, p, err)
					return
				}
				resp.Body.Close()
			}(peer)
		}
	}
}

// watchdog promotes this node to leader if it has not received a ping within
// failureThreshold.
func (n *Node) watchdog() {
	for {
		time.Sleep(watchdogInterval)
		n.mu.Lock()
		isLeader := n.isLeader
		since := time.Since(n.lastPing)
		n.mu.Unlock()

		if !isLeader && since > failureThreshold {
			fmt.Printf("[%s] No ping for %v — starting election\n", n.id, since.Round(time.Millisecond))
			n.becomeLeader()
		}
	}
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node1"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	peers := splitCSV(os.Getenv("PEERS"))

	n := &Node{
		id:       nodeID,
		peers:    peers,
		lastPing: time.Now(),
	}

	// node1 bootstraps as the initial leader.
	if nodeID == "node1" {
		n.isLeader = true
		n.term = 1
		fmt.Printf("[%s] Starting as initial leader (term %d)\n", nodeID, n.term)
	} else {
		fmt.Printf("[%s] Starting as follower, watching for leader heartbeats\n", nodeID)
	}

	http.HandleFunc("/ping", n.handlePing)
	http.HandleFunc("/status", n.handleStatus)

	go n.sendPings()
	go n.watchdog()

	fmt.Printf("[%s] HTTP server on :%s\n", nodeID, port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
