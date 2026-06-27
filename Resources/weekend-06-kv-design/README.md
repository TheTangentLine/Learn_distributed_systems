# Weekend 6 — Distributed Key-Value Store: Design Document

**Week 6 weekend project.** This is a paper/whiteboard exercise. The goal is to design a key-value store that uses N=3 replication before writing a single line of code. The implementation follows in Week 8 (`weekend-08-kv-store`).

---

## Functional requirements

- `PUT /key/{k}` — write a value
- `GET /key/{k}` — read a value
- `DELETE /key/{k}` — delete a key
- Keys are arbitrary strings; values are arbitrary strings

## Non-functional requirements

- Survive 1 node failure without losing data or availability
- Writes should be durable on at least 2 nodes before acknowledging
- Reads should always return the most recent write (or a close approximation)

---

## Replication parameters

| Parameter | Value | Reasoning |
|-----------|-------|-----------|
| N | 3 | 3 replicas; can tolerate 1 node failure |
| W | 2 | Write must reach 2 nodes; `W + R > N` for overlap |
| R | 2 | Read must query 2 nodes; ensures at least one saw the last write |

### Quorum math

With N=3, W=2, R=2:

- `W + R = 4 > N = 3` — every read set overlaps with every write set by at least 1 node.
- A write can succeed even if 1 of 3 nodes is down (only 2 acks needed).
- A read can succeed even if 1 of 3 nodes is down (only 2 responses needed).
- If 2 nodes are down, both writes and reads fail (2 < W=2 acks available).

---

## Data routing

Keys are distributed using a consistent hash ring (from Weekend 5):

1. Hash the key to a position on the ring.
2. Walk clockwise to find the first node — this is the primary.
3. Continue clockwise for N-1 more distinct nodes — these are the replicas.

Result: `ring.GetN(key, 3)` returns `[NodeA, NodeC, NodeD]` for a given key.

---

## Write path (PUT)

```
Client
  │ PUT /key/username value="Kha"
  ▼
Coordinator node (receives the request)
  │ 1. Gets current value's vector clock (VC) or empty VC if new key
  │ 2. Increments VC[coordinatorID]
  │ 3. Forwards Entry{value, VC} to all N=3 replica nodes in parallel
  │    via PUT /internal/set/username
  │ 4. Waits for W=2 acks (timeout 500ms)
  │ 5. Returns 200 if acks >= W, 500 otherwise
  ▼
Client receives confirmation
```

**What if 1 replica is slow?** The coordinator waits for W=2 acks. If the 3rd replica hasn't acked within 500ms, it still proceeds. The 3rd replica will receive the write eventually (async replication or read repair).

---

## Read path (GET)

```
Client
  │ GET /key/username
  ▼
Coordinator node
  │ 1. Asks all N=3 replicas for their copy via GET /internal/get/username
  │ 2. Waits for R=2 responses (timeout 500ms)
  │ 3. Compares vector clocks of responses
  │ 4. Returns the value with the highest (most recent) vector clock
  │ 5. (Read repair) If one replica had an older VC, async-send the fresh
  │    value to it via PUT /internal/set
  ▼
Client receives the most recent value
```

---

## Conflict detection with vector clocks

Each stored value carries a vector clock `map[nodeID]int`.

- When Node A writes, it increments `VC["A"]`.
- When Node B reads and then writes (based on Node A's value), it merges Node A's VC and increments `VC["B"]`.
- If Node A and Node B write concurrently (neither saw the other's write), their VCs are **concurrent** — neither dominates. The coordinator detects this and can return both values, letting the application resolve the conflict (or apply LWW as a fallback).

---

## Failure scenarios

### Scenario A — 1 replica down during write

- Coordinator attempts all 3 writes in parallel.
- 2 succeed, 1 times out.
- W=2 is met → write succeeds.
- The downed replica misses the write. When it comes back, read repair or anti-entropy will heal it.

### Scenario B — 1 replica down during read

- Coordinator asks all 3 replicas.
- 2 respond, 1 times out.
- R=2 is met → read succeeds using the 2 available values.
- If both responding replicas have the same VC, return the value.
- If one is stale (lower VC), trigger read repair asynchronously.

### Scenario C — Network partition splits nodes 2-1

- Node C is isolated from Node A and Node B.
- A client writes to Node A: coordinator contacts A, B, C. A and B ack → W=2 met → write succeeds.
- Node C misses the write.
- A client reads from Node C acting as coordinator: contacts A, B, C. A and B respond with the new value, C responds with old value → R=2 met, returns new value, triggers read repair on C.

---

## Questions to answer before implementing

1. Where does the coordinator role live? Is it a dedicated node, or does any node act as coordinator for keys it receives?

   _Answer: any node acts as coordinator for the request it receives. It routes to the correct replica nodes using the consistent hash ring._

2. How does a new node join the cluster and receive its share of keys?

   _Answer: for this implementation, the node list is static (configured via env var). Dynamic membership (gossip + rebalancing) is out of scope._

3. What happens to the vector clock when a key is deleted?

   _Answer: a tombstone entry is written (a special `deleted: true` flag with an incremented VC). Reads that see the tombstone return 404. The tombstone must be kept until all replicas have seen it._

---

## Next step

Implement this design in `weekend-08-kv-store/`. That project contains the full Go implementation including read repair.
