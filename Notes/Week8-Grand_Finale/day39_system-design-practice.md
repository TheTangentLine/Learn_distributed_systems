# Day 39: System Design Practice — URL Shortener

## The 5-Step Framework

Every system design interview should follow this structure. Do not jump to components before understanding the requirements.

1. **Clarify requirements** — functional and non-functional
2. **Estimate scale** — QPS, storage, bandwidth
3. **Design API + high-level components**
4. **Deep dive** — partitioning, caching, consistency
5. **Failure modes** — what happens when X is down?

---

## Problem: Design a URL Shortener

A service like bit.ly: given a long URL, return a short code (e.g., `bit.ly/3kQjF2`). Given a short code, redirect to the original URL.

### Step 1 — Clarify requirements

**Functional:**
- `POST /shorten` with a long URL → returns a short code
- `GET /{code}` → 302 redirect to the original URL
- Optionally: custom aliases, expiry, analytics (click counts)

**Non-functional:**
- High read availability (redirects are on the critical path for every link click)
- Low latency for redirect (<10ms p99)
- Durability (short codes must not disappear)
- Scale: assume 100M short links total, 1000 writes/sec, 50,000 reads/sec

### Step 2 — Estimate scale

```
Reads:  50,000 req/sec × 86,400 sec/day = 4.3 billion reads/day
Writes: 1,000 req/sec × 86,400 sec/day  = 86 million writes/day

Storage per record: code(7B) + url(200B) + metadata(50B) ≈ 257 bytes
100M records × 257 bytes = ~25 GB total — fits on a single SSD but not in RAM

Cache: cache top 1% of URLs (Pareto: 1% of URLs get 99% of traffic)
  1% × 100M = 1M records × 200B = 200 MB — fits in Redis on one node
```

### Step 3 — API and high-level components

```
Client → CDN/Edge → Load Balancer → App Servers → Cache (Redis) → DB (PostgreSQL + sharding)
```

**Shortening:**
1. App server receives long URL.
2. Generates a 7-character base-62 code (62^7 = 3.5 trillion unique codes).
3. Writes `(code, url, created_at, user_id)` to the database.
4. Returns short URL.

**Redirecting:**
1. App server receives short code.
2. Checks Redis cache. Cache hit → return URL immediately.
3. Cache miss → query DB → populate cache → return URL.

**Code generation options:**
- Random: generate a random 7-char base-62 string, check for collision in DB.
- Hash-based: `base62(md5(longURL)[:7])` — deterministic but two URLs can collide.
- Counter-based: auto-increment ID → convert to base-62. Predictable (guessable) but no collision risk.

### Step 4 — Deep dive

**Partitioning:**

If writes exceed one DB node's capacity (unlikely for URL shortener but good practice), shard by the first character of the code (base-62, 62 shards) or by a hash of the code.

**Caching:**

Redis with a TTL of 24 hours. Eviction policy: LRU. Pre-warm top 1000 URLs at service startup.

**Read-your-writes:**

After shortening, the user might click the link immediately. If reads go to a replica before replication completes, they get a 404. Fix: route the user's first read to the primary for 1 second after shortening (use a cookie or JWT timestamp).

**Analytics:**

Don't count clicks in the database on every request (write amplification). Instead, publish to Kafka on each redirect and aggregate asynchronously.

### Step 5 — Failure modes

| Failure | Impact | Mitigation |
|---------|--------|-----------|
| App server crashes | Other servers handle traffic via LB health checks | Multiple app servers, auto-scaling |
| Redis is down | Every redirect hits the DB (cache miss) | DB is sized for this load; add Redis cluster with replica |
| DB primary is down | Writes fail; reads continue from replica | Automatic failover (Patroni, AWS RDS Multi-AZ); accept brief write downtime |
| DB replica is down | Reads route to primary temporarily | Load balancer health checks; multiple read replicas |
| DDoS on one short code | Hot key problem; overwhelms one DB shard | Redis absorbs most traffic; rate limit per IP at CDN layer |

---

## Hands-on Assignment (Go)

Write a 1-page design document answering each of the 5 steps for one of these alternative problems. Pick one:

**Option A:** Design a rate limiter (like the one protecting the `/charge` endpoint from Day 32).

**Option B:** Design a distributed job scheduler (like a cron service across a cluster).

**Option C:** Design the backend for a real-time leaderboard for a mobile game.

Create `my_design_doc.md` in this folder. Follow the same 5-step structure.

---

## Review

1. Why is the code generation strategy (random vs hash vs counter) actually a security concern, not just an engineering concern?

2. The system serves 50,000 redirects/sec. Each redirect checks Redis (0.5ms) and potentially the DB (5ms). What is the throughput bottleneck? How many app servers do you need to sustain 50,000 req/sec if each server handles 2000 req/sec?
