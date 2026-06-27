# System Design: Rate Limiter

> Classic difficulty: Medium. Tests distributed coordination, atomicity, and failure trade-offs.

---

## Step 1 — Clarify Requirements

**Functional:**
- Limit the number of requests a client can make within a time window
- Return HTTP 429 (Too Many Requests) when the limit is exceeded
- The limit is scoped per: user ID, API key, or IP address (clarify which)
- Configurable limits per endpoint (e.g., `/login` stricter than `/profile`)

**Non-functional:**
- Latency overhead of the rate limiter must be < 2ms p99 (cannot slow down every request)
- Accuracy: a few extra requests slipping through is acceptable; locking out a legitimate user is not
- Scale: 500,000 API calls/sec across our fleet of 100 app servers

**Out of scope:** billing tiers, admin UI, analytics dashboard

---

## Step 2 — Estimate Scale

```
Requests: 500,000/sec across 100 servers = 5,000/sec per server
Redis operations: 1–2 per request (GET + INCR) = 1,000,000 ops/sec
Redis single node throughput: ~200K ops/sec per CPU core
→ Need a Redis Cluster with 5–10 nodes

Memory per counter (e.g., 60-second window):
  key="rate:user:123" → 8 bytes key + 8 bytes counter + 4 bytes TTL ≈ 20 bytes
  10M active users × 20 bytes = 200 MB → fits in RAM comfortably
```

---

## Step 3 — High-Level Design

```
Client Request
  │
  ▼
App Server
  │
  ├── 1. Extract identity key (user_id / api_key / ip)
  ├── 2. Call rate-limit middleware
  │       │
  │       ▼
  │   Redis Cluster (counters)
  │       │
  │       ├── Under limit? → allow, return 200
  │       └── Over limit?  → return 429 + Retry-After header
  │
  └── 3. Forward to business logic
```

**Response headers to always include:**

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 23
X-RateLimit-Reset: 1719482400
Retry-After: 42   (only on 429)
```

---

## Step 4 — Deep Dive

### Algorithm 1: Token Bucket

```
Bucket holds up to `capacity` tokens.
Tokens refill at `rate` tokens/second continuously.
Each request consumes 1 token.
If the bucket is empty → reject (429).
```

**Properties:**
- Allows **bursting** up to `capacity` — good for APIs that expect occasional spikes
- Smooth refill means the bucket can never be "gamed" by sending all requests at the exact window boundary

**Implementation (pseudo):**
```go
type TokenBucket struct {
    mu          sync.Mutex
    tokens      float64
    maxTokens   float64
    refillRate  float64  // tokens per second
    lastRefill  time.Time
}

func (b *TokenBucket) Allow() bool {
    b.mu.Lock()
    defer b.mu.Unlock()
    now := time.Now()
    elapsed := now.Sub(b.lastRefill).Seconds()
    b.tokens = min(b.maxTokens, b.tokens + elapsed*b.refillRate)
    b.lastRefill = now
    if b.tokens >= 1 {
        b.tokens--
        return true
    }
    return false
}
```

**Limitation:** state is in-process. Does not work across multiple app servers without a shared store.

---

### Algorithm 2: Sliding Window Log

```
Store a sorted set (timestamp log) of all request times for a user.
On each request:
  1. Delete all entries older than now - window
  2. Count remaining entries
  3. If count < limit → allow + add current timestamp
  4. Else → reject
```

**Properties:**
- **Precise:** no boundary burst (unlike fixed window)
- **Memory heavy:** stores one entry per request, not just one counter

**Redis implementation:**

```bash
# Key: "swlog:user:123"
ZREMRANGEBYSCORE swlog:user:123 0 (now_ms - window_ms)
count = ZCARD swlog:user:123
if count < limit:
    ZADD swlog:user:123 now_ms now_ms
    EXPIRE swlog:user:123 window_s
    allow
else:
    reject
```

**Problem:** 3 Redis commands → not atomic. Fix with a Lua script (executed atomically by Redis):

```lua
-- rate_limiter.lua
local key     = KEYS[1]
local now     = tonumber(ARGV[1])     -- current time in ms
local window  = tonumber(ARGV[2])     -- window in ms
local limit   = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, now)
    redis.call('EXPIRE', key, math.ceil(window / 1000))
    return 1  -- allowed
end
return 0      -- rejected
```

Call with `EVALSHA <sha> 1 rate:user:123 <now_ms> 60000 100`.

---

### Algorithm 3: Fixed Window with Redis INCR (Simplest, Production-Grade)

```bash
key = "rate:user:123:minute:2024-01-01T12:05"
count = INCR key
if count == 1:
    EXPIRE key 60     # set TTL only on first increment
if count > limit:
    return 429
```

**Two-command atomicity issue:** if the process crashes between `INCR` and `EXPIRE`, the key never expires and the user is permanently rate-limited. Fix: use `SET key 0 EX 60 NX` to initialise with TTL, then `INCR`.

**Boundary burst problem:** a user can make `2×limit` requests in 2 seconds by hitting the end of window N and the start of window N+1. Acceptable for most APIs.

---

### Distributed Rate Limiter (Multiple App Servers)

| Approach | Consistency | Latency overhead | Notes |
|----------|-------------|-----------------|-------|
| **Centralised Redis** | Strong | +1–2ms per request | Standard choice; use pipelining |
| **Sticky sessions** | Strong per user | 0 (local counter) | Single server per user; bad for failover |
| **Local counter + gossip sync** | Eventual | ~0 extra latency | Each node holds a local count; gossip propagates totals every 100ms. A user may exceed the limit briefly across nodes. Acceptable for most use cases. |

For most production systems: **centralised Redis with a Lua script** is the right answer.

---

### Clock Skew Across Nodes

Fixed-window keys include a timestamp. If node A thinks it is 12:05:00 and node B thinks it is 12:04:59 (1 second behind), a request could be counted in different windows.

Mitigations:
- Use Redis server time (`TIME` command) as the authoritative clock — all app servers use the same clock source
- Allow a 1-second grace overlap when computing the key

---

## Step 5 — Failure Modes

| Component | Failure | Impact | Mitigation |
|-----------|---------|--------|-----------|
| Redis node down | Rate limit checks fail | Cannot accept OR reject requests correctly | **Fail open:** allow all requests (revenue preserved, security degrades). **Fail closed:** reject all requests (secure, bad UX). Choose based on use case. |
| Redis latency spike | Rate limit check > 10ms | Adds latency to every API call | Timeout + fail open after 5ms; circuit breaker on Redis calls |
| App server crash mid-increment | `INCR` ran but `EXPIRE` not set | Key never expires; user locked out permanently | Use `SET key 0 EX 60 NX` + `INCR` pattern (TTL set at creation, not after) |
| Clock skew | Boundary burst or double counting | Slightly inaccurate counts near window edges | Use Redis `TIME` command as authoritative clock source |

---

## Key Talking Points for the Interview

1. **Algorithm choice is a trade-off:** token bucket for burst-friendly APIs (e.g., search), sliding window log for strict billing-tier enforcement.
2. **Atomicity with Lua:** explain that Redis executes Lua scripts atomically — no other command can interleave. This is the correct production solution.
3. **Fail open vs fail closed:** this is a business decision, not an engineering one. Payment APIs fail closed; social APIs fail open.
4. **The boundary burst problem** of fixed windows is a real issue but acceptable for most use cases — mentioning it shows depth.
