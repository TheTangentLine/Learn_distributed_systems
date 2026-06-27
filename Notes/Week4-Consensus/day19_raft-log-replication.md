# Day 19: Raft — Log Replication

## 1. The Log

Every Raft node maintains an **append-only log** of commands. The leader appends new entries, replicates them to followers, and once a majority has the entry, marks it **committed**.

A log entry has three fields:

- `Index` — its position in the log (1-based, monotonically increasing)
- `Term` — the term number when the leader created this entry
- `Command` — the actual operation (e.g., `SET x=5`)

## 2. AppendEntries RPC

The leader periodically sends `AppendEntries` to every follower:

- **As a heartbeat** (empty entries, just to assert leadership).
- **To replicate new log entries.**

The follower accepts the entries if:
- The leader's term ≥ its current term.
- The previous log entry (identified by `prevLogIndex` and `prevLogTerm`) matches its own log — **Log Matching Property**.

If the follower's log diverges (it has stale entries from an old term), the leader sends earlier entries until it finds the common point, then overwrites the divergent entries.

## 3. Commitment

```
Leader appends entry → replicates to majority → commits → applies to state machine → responds to client
```

An entry is committed once the leader has received `AppendEntries` acknowledgment from a majority. The leader then advances its `commitIndex` and notifies followers in the next heartbeat, so they also apply the entry.

**Safety rule:** the leader only commits entries from its **current term** by counting majority replicas. This prevents a tricky edge case where a deposed leader's old entries get incorrectly committed by a new leader.

---

## Hands-on Assignment (Go)

We extend the Day 18 Raft node to support log entries. Three nodes replicate 5 writes and we verify they all converge to the same log.

### Step 1: Add log to `raft.go`

Add these types and fields to the Day 18 code:

```go
type LogEntry struct {
	Term    int
	Index   int
	Command string
}

// Add to RaftNode struct:
//   log         []LogEntry
//   commitIndex int
//   nextIndex   map[int]int  // leader only: next index to send to each peer

func (n *RaftNode) AppendLog(command string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return false
	}
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   len(n.log) + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	fmt.Printf("Leader %d appended: [%d/%d] %s\n", n.id, entry.Index, entry.Term, entry.Command)
	return true
}
```

### Step 2: Replicate in heartbeats

In `sendHeartbeats`, send the latest log entries to each follower:

```go
func (n *RaftNode) sendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	entries := append([]LogEntry{}, n.log...)
	n.mu.Unlock()

	acks := 1 // leader counts itself
	for _, peer := range n.peers {
		if peer.replicateEntries(term, n.id, entries) {
			acks++
		}
	}

	// Commit if majority acked
	if acks > len(n.peers)/2+1 {
		n.mu.Lock()
		n.commitIndex = len(n.log)
		n.mu.Unlock()
	}
}

func (n *RaftNode) replicateEntries(leaderTerm, leaderID int, entries []LogEntry) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if leaderTerm < n.currentTerm {
		return false
	}
	n.currentTerm = leaderTerm
	n.state = Follower
	n.log = append([]LogEntry{}, entries...)
	select { case n.heartbeat <- struct{}{}: default: }
	return true
}
```

### Step 3: Full test in `main()`

```go
func main() {
	// ... (same node setup as Day 18) ...

	time.Sleep(500 * time.Millisecond) // wait for election

	// Find the leader
	var leader *RaftNode
	for _, n := range nodes {
		n.mu.Lock()
		if n.state == Leader {
			leader = n
		}
		n.mu.Unlock()
	}
	if leader == nil {
		fmt.Println("No leader elected!")
		return
	}

	// Send 5 writes to the leader
	commands := []string{"SET x=1", "SET y=2", "SET x=3", "DEL y", "SET z=5"}
	for _, cmd := range commands {
		leader.AppendLog(cmd)
		time.Sleep(120 * time.Millisecond) // allow replication heartbeat
	}

	time.Sleep(300 * time.Millisecond)

	// Verify all nodes have the same log
	fmt.Println("\n=== Log comparison ===")
	for _, n := range nodes {
		n.mu.Lock()
		fmt.Printf("Node %d log: ", n.id)
		for _, e := range n.log {
			fmt.Printf("[%d:%s] ", e.Index, e.Command)
		}
		fmt.Println()
		n.mu.Unlock()
	}
}
```

### Step 4: Run and verify

```bash
go run raft.go
```

All three nodes should print the same 5 log entries in the same order.

---

## Review

1. The leader has log entries 1-5. Follower A has entries 1-3 (crashed and missed 4-5). Follower B has entries 1-5 plus a stale entry 6 from an old term. How does the leader reconcile each follower?

2. Why is it unsafe for a new leader to commit entries from a previous term just by counting how many followers have them? (This is the most subtle safety property in Raft — look up the "Figure 8 scenario" in the Raft paper.)
