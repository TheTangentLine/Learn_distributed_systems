# Day 40: Distributed KV Store — Design

## 1. Final Project Brief

You are going to build a **distributed key-value store** that incorporates the core concepts from all 8 weeks. Today is the design day. Days 41–42 are implementation.

### Requirements

- **HTTP API:** `PUT /key/{k}`, `GET /key/{k}`, `DELETE /key/{k}`
- **Consistent hashing ring** to route keys to nodes
- **N=3 replication:** every write is sent to 3 nodes
- **Quorum:** W=2 for writes, R=2 for reads
- **Vector clocks** on each value to detect conflicts
- **Inter-node forwarding:** a node that receives a write it doesn't own forwards it to the correct node
- **Node list** configured via environment variable

---

## 2. API Contract

```
PUT /key/{k}
  Body: {"value": "string"}
  Response: 200 {"ok": true, "vector_clock": {...}}

GET /key/{k}
  Response: 200 {"value": "string", "vector_clock": {...}}
           404 if key not found on R nodes

DELETE /key/{k}
  Response: 200 {"ok": true}

GET /internal/get/{k}    (inter-node, not exposed to clients)
PUT /internal/set/{k}    (inter-node, not exposed to clients)
```

## 3. Data Flow — PUT /key/foo

```mermaid
flowchart LR
    C["Client"] -->|PUT /key/foo value=bar| N1["Node 1\n(receives request)"]
    N1 -->|ring.GetNodes('foo', 3)| Ring["Consistent Hash Ring"]
    Ring -->|N2, N3, N4| N1
    N1 -->|PUT /internal/set/foo| N2["Node 2"]
    N1 -->|PUT /internal/set/foo| N3["Node 3"]
    N1 -->|PUT /internal/set/foo| N4["Node 4"]
    N2 -->|ack| N1
    N3 -->|ack| N1
    N1 -->|W=2 acks received, respond| C
```

## 4. Vector Clock Design

Each stored value has an associated vector clock: `map[nodeID]int`.

- When Node i writes a value, it increments `VC[i]`.
- On `GET`, the coordinator collects R responses and returns the value with the **highest** vector clock.
- If two responses have concurrent vector clocks (neither dominates), return both and let the client resolve (or use LWW as a fallback).

## 5. Three Failure Scenarios to Handle

### Scenario A — One replica is down during write

With W=2 and N=3, a write should succeed even if 1 replica is unavailable. The coordinator attempts all 3 writes in parallel; if at least 2 succeed, it responds OK.

### Scenario B — One replica is down during read

With R=2, a read should succeed even if 1 replica is unavailable. The coordinator collects responses from available replicas and picks the one with the highest vector clock.

### Scenario C — Split read (stale replica)

Two nodes return different values for the same key (replica N3 missed a write). The coordinator detects the discrepancy via vector clock comparison and triggers **read repair**: it sends the fresh value to the stale replica asynchronously before returning the fresh value to the client.

---

## Hands-on Assignment (Go)

Create the project skeleton and make sure the design is concrete before coding.

### Step 1: Create the project

```bash
mkdir dist-sys-day40
cd dist-sys-day40
go mod init kvstore
```

### Step 2: Sketch the module structure

```
kvstore/
  main.go          — HTTP server startup, reads NODE_LIST from env
  ring.go          — consistent hash ring (reuse from Day 24)
  store.go         — in-memory KV store with vector clock
  handler.go       — PUT/GET/DELETE HTTP handlers (client-facing)
  internal.go      — /internal/set, /internal/get handlers (inter-node)
  coordinator.go   — quorum write and read logic
  vectorclock.go   — vector clock struct (reuse from Day 13)
```

### Step 3: Write the data structure types (types.go)

```go
package main

type VectorClock map[string]int

func (vc VectorClock) Increment(nodeID string) VectorClock {
    result := make(VectorClock)
    for k, v := range vc { result[k] = v }
    result[nodeID]++
    return result
}

// Returns: -1 (a < b), 1 (a > b), 0 (concurrent)
func (a VectorClock) Compare(b VectorClock) int {
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

type Entry struct {
    Value string      `json:"value"`
    VC    VectorClock `json:"vector_clock"`
}
```

### Step 4: Plan the coordinator write logic

Pseudocode for `coordinatorPut(key, value string)`:

```
1. nodes = ring.GetN(key, 3)  // 3 replica nodes
2. newVC = localStore.Get(key).VC.Increment(myNodeID)
3. entry = Entry{value, newVC}
4. for each node in nodes:
     go PUT /internal/set/{key} body=entry → collect acks
5. wait for W=2 acks (with timeout 500ms)
6. if acks >= W: respond 200
   else: respond 500 "quorum not reached"
```

---

## Review

1. Why do we send the write to all N=3 replicas and wait for only W=2 acks, rather than sending to only W=2 replicas?

2. What happens to the third replica (the one that didn't ack in time) — does it eventually get the write? When and how?
