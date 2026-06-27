# System Design: Distributed Cache

> Classic difficulty: Medium–Hard. Tests consistent hashing, cache coherency, and failure resilience.

---

## Step 1 — Clarify Requirements

**Functional:**
- `SET key value [TTL]` — store a value with optional expiry
- `GET key` — retrieve a value; return null on miss
- `DELETE key` — evict a key
- The cache is a standalone distributed service (not co-located with the application)

**Non-functional:**
- Sub-millisecond p99 read/write latency
- Linear scalability — adding nodes should increase capacity and throughput proportionally
- High availability — cache failures should degrade gracefully, not crash the application
- Scale: 50 cache nodes, 10 TB total cache space, 5M ops/sec

**Out of scope:** persistence (cache is ephemeral by definition), complex data types

---

## Step 2 — Estimate Scale

```
5M ops/sec across 50 nodes = 100K ops/sec per node
In-memory: a single Redis/Memcached node handles ~200K simple ops/sec → within budget

Capacity per node:
  10 TB total / 50 nodes = 200 GB per node
  A high-memory EC2 instance (r6g.4xlarge) has 128 GB RAM → scale to 80 nodes
  Or use SSD-backed cache (Redis on Flash) for cold tier

Key space:
  Average key+value: 500 bytes
  200 GB / 500 bytes = 400M keys per node → 20B keys total
```

---

## Step 3 — High-Level Design

```
Application Servers
  │
  ├── Cache Client (consistent hash ring, embedded in app)
  │       │
  │       ├── hash(key) → node N
  │       │
  │       └── gRPC / binary protocol to node N
  │
  ├─────────────────────────────────────────────
  │
  ▼
Cache Nodes (N=50)
  │   Node 1   Node 2   ...   Node 50
  │  [keys]   [keys]         [keys]
  │
  └── each node is a primary with 1 replica on the next node in the ring
```

The **cache client** lives in the application (as a library) and contains the consistent hash ring. There is no proxy hop — direct routing to the correct node.

---

## Step 4 — Deep Dive

### Consistent Hashing for Node Assignment

Use a hash ring (see `Resources/weekend-05-consistent-hashing`) to map keys to nodes:

```
1. Hash all 50 node IDs onto a circle [0, 2^32)
2. For each key: hash(key) → point on the circle → walk clockwise to find the node
3. Use virtual nodes (150 vnodes per physical node) for uniform distribution
```

**Why consistent hashing (not modulo)?**

With `node = hash(key) % N`:
- Adding one node remaps `N/(N+1)` fraction of all keys → massive cache miss storm
- With consistent hashing: only `1/N` of keys move when one node is added/removed

**Reference:** `Resources/weekend-05-consistent-hashing/ring.go`

---

### Cache Access Patterns

#### Cache-aside (Lazy Loading)

```
Read:
  v = cache.Get(key)
  if v == nil:
    v = db.Get(key)
    cache.Set(key, v, ttl=300s)
  return v

Write:
  db.Write(key, value)
  cache.Delete(key)   ← invalidate, don't update (avoids race condition)
```

**Race condition (why invalidate instead of update):**
```
Thread 1: db.Write(key, "v2")
Thread 2: cache.Get(key) misses, reads db (gets stale "v1"), calls cache.Set(key, "v1")
Thread 1: cache.Delete(key)   ← too late: "v1" is now in cache
```
Fix: always delete on write, never update; the next reader re-populates from DB.

#### Write-through

```
Write:
  cache.Set(key, value)
  db.Write(key, value)     ← synchronous
```

**Pros:** cache is always fresh. **Cons:** every write hits the DB (no write batching benefit).

#### Write-behind (Write-back)

```
Write:
  cache.Set(key, value)
  queue.Publish(key, value)   ← async

Worker:
  db.Write(key, value)
```

**Pros:** very low write latency. **Cons:** data loss if cache node dies before the async write completes.

---

### Thundering Herd (Dog-pile Effect)

**Problem:** A popular key expires at time T. Simultaneously, 10,000 requests all get a cache miss and all rush to the DB. The DB is overloaded for the next 500ms until the first request repopulates the cache.

**Mitigation 1: Mutex on cache miss (cache lock)**

```go
func Get(key string) (string, error) {
    v, err := cache.Get(key)
    if err == nil {
        return v, nil
    }
    // Cache miss — acquire a distributed lock for this key
    lock := redis.SetNX("lock:"+key, 1, 5*time.Second)
    if !lock {
        // Another goroutine is fetching — wait and retry
        time.Sleep(50 * time.Millisecond)
        return Get(key)   // retry; cache should be warm now
    }
    defer redis.Del("lock:" + key)
    v = db.Get(key)
    cache.Set(key, v, ttl)
    return v, nil
}
```

**Mitigation 2: Probabilistic early expiry (PER)**

```
When a cache hit occurs, check if the key is "close to expiry":
  if remaining_ttl < threshold AND random() < expiry_probability:
    refresh the cache in the background
```

Spread the refresh load over time instead of all at once.

**Mitigation 3: Short-lived placeholder**

On cache miss, immediately set a placeholder `{loading: true}` with a 1-second TTL. Subsequent requests see the placeholder and return a "retry" response. Only one request hits the DB.

---

### Eviction Policies

| Policy | Description | Best for |
|--------|-------------|---------|
| **LRU** (Least Recently Used) | Evict the key that was accessed longest ago | General-purpose; good locality of reference |
| **LFU** (Least Frequently Used) | Evict the key accessed fewest times | Long-lived keys that are rarely used (avoids keeping "one-hit wonders") |
| **TTL-based** | Keys expire after a set time, regardless of access | Data with natural staleness (sessions, rate-limit counters) |
| **FIFO** | Evict oldest-inserted key | Rare; only when insertion order matters |

Redis default: `allkeys-lru`. For most caches, LRU + TTL is the right combination.

---

### Replication for Hot Keys

A single key read by 5M requests/sec (e.g., the front-page hero content) will saturate one cache node:
- Replicate the hot key to multiple nodes: `key → {key_replica_0, key_replica_1, ..., key_replica_9}`
- Client picks a random replica on each read: `pick = hash(key) + random(0, 9)`
- Write to all replicas; accept stale reads for up to 100ms (eventual consistency)

---

## Step 5 — Failure Modes

| Component | Failure | Impact | Mitigation |
|-----------|---------|--------|-----------|
| Cache node dies | All keys on that node are lost | Thundering herd on downstream DB for affected keys | 1 replica per node on ring; consistent hashing routes to neighbour on node death |
| Cache client crashes | One app server misroutes | No cache access from that server | Client is stateless; restart reconnects |
| Cache full (eviction pressure) | Hot keys evicted prematurely | DB load increases | Monitor `evicted_keys` metric; scale up or add nodes |
| Network partition between app and cache | Cache unreachable | All reads go to DB | Cache client: fail open (fall through to DB with a circuit breaker + timeout) |
| Hot key overload | Single node overwhelmed by popular key | p99 latency spike on that node | Replicate hot keys across multiple nodes; use read replicas |

---

## Key Talking Points for the Interview

1. **Consistent hashing is why distributed caches scale** — not just for load distribution, but to minimise key movement on topology changes.
2. **Cache-aside + invalidate-on-write** is the safest pattern. The race condition with cache.Set on write is a common interview trap.
3. **Thundering herd** is the most interesting failure mode — have two mitigations ready (lock vs PER).
4. **Virtual nodes** in consistent hashing are essential — without them, removing one physical node puts all its load on exactly one neighbour.
