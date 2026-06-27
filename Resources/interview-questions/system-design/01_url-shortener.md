# System Design: URL Shortener

> Classic difficulty: Easy–Medium. Tests caching, partitioning, and idempotency.

---

## Step 1 — Clarify Requirements

**Functional:**
- `POST /shorten` — given a long URL, return a unique short code (e.g., `bit.ly/3kQjF2`)
- `GET /{code}` — redirect to the original URL (HTTP 302)
- Optional: custom aliases, link expiry, per-link analytics

**Non-functional:**
- High read availability — redirects are on the critical path of every link click
- Low latency — p99 redirect < 10ms
- Durability — short codes must never disappear
- Scale: 100M total links, 1,000 writes/sec, 50,000 reads/sec

**Out of scope:** user authentication, billing, A/B testing

---

## Step 2 — Estimate Scale

```
Reads:   50,000 req/sec
Writes:  1,000  req/sec
Read:write ratio = 50:1  → heavily read-bound → caching is the key decision

Storage per record:
  code (7 bytes) + url (200 bytes avg) + metadata (50 bytes) ≈ 257 bytes
  100M records × 257 bytes ≈ 25 GB total
  → fits on a single SSD, but NOT in RAM

Cache sizing:
  Pareto principle: 1% of URLs get 99% of traffic
  1% × 100M × 200 bytes = 200 MB → fits comfortably in one Redis node

Bandwidth:
  Read:  50,000 × 257 bytes ≈ 13 MB/s → trivial
  Write: 1,000  × 257 bytes ≈  0.3 MB/s
```

---

## Step 3 — High-Level Design

```
Client
  │
  ▼
CDN / Edge cache (cache 302 redirects for popular codes)
  │
  ▼
Load Balancer
  │
  ├──────────────────────────────────┐
  ▼                                  ▼
App Server (shorten)          App Server (redirect)
  │                                  │
  ▼                                  ▼
PostgreSQL DB              Redis Cache (LRU, TTL=24h)
(source of truth)                    │
                               Cache miss → DB
```

**Shorten flow:**
1. App server receives `POST /shorten` with a long URL.
2. Generates a 7-character base-62 code.
3. Writes `(code, url, created_at, user_id)` to the database.
4. Returns the short URL.

**Redirect flow:**
1. App server receives `GET /{code}`.
2. Checks Redis cache. Cache hit → return 302 immediately.
3. Cache miss → query DB → populate Redis → return 302.

---

## Step 4 — Deep Dive

### Code generation strategies

| Strategy | Pros | Cons | Use when |
|----------|------|------|----------|
| Random base-62 | No collision risk with DB check; unpredictable | DB round-trip on collision | Default choice |
| Hash-based `md5(url)[:7]` | Deterministic; dedup identical URLs naturally | Rare hash collisions still possible | When dedup is a requirement |
| Auto-increment counter → base-62 | Zero collisions; simple | Sequential codes are guessable (security risk) | Internal tooling only |

**Security note:** counter-based codes expose your write volume (`bit.ly/7` was created before `bit.ly/8`) and allow enumeration attacks. Random codes prevent both.

### Database sharding

25 GB fits on one node today, but plan for growth:

- Shard by `hash(code) % N` → uniform distribution, no hot spots
- Use consistent hashing (`Resources/weekend-05-consistent-hashing`) for smooth rebalancing
- Alternatively: 62 shards by first character of the base-62 code (natural and deterministic)

### Caching strategy

- **Policy:** cache-aside (read-through)
- **Eviction:** LRU; 200 MB cache fits on a single Redis node
- **TTL:** 24 hours. Expired links re-query DB on next hit.
- **Pre-warm:** on startup, load top-1000 most-clicked codes into Redis from analytics

### Read-your-writes consistency

After shortening, the user immediately clicks the link. If the redirect goes to a replica that hasn't replicated the new row yet, they get a 404.

Fix: for 1 second after a write, route that user's reads to the primary. Implement via a cookie `recent_write=<timestamp>` or a JWT claim. The app server checks: "was this user's last write < 1 second ago?" If yes, read from primary.

### Analytics

Do **not** `UPDATE clicks = clicks + 1` on every redirect — that is a write per read (50,000 writes/sec to the DB). Instead:

1. On each redirect, publish an event to Kafka: `{code, user_agent, referrer, timestamp}`
2. A batch consumer aggregates click counts every 60 seconds and writes to an analytics table

---

## Step 5 — Failure Modes

| Component | Failure | Impact | Mitigation |
|-----------|---------|--------|-----------|
| App server | Crashes | Load balancer detects via health check within 10s; routes to other servers | Run ≥ 3 app servers; auto-scaling group |
| Redis | Down | Every redirect hits the DB (cache miss storm, 50K reads/sec) | DB is sized for 50K reads/sec; add Redis replica; circuit breaker to fail-open |
| DB primary | Down | Writes fail (shorten broken); reads continue from replica | Patroni / AWS RDS Multi-AZ for automatic failover; accept < 30s write downtime |
| DB replica | Down | Reads degrade; route to primary temporarily | Multiple read replicas; load balancer health checks |
| Hot short code | Single code gets 100K redirects/sec | Overwhelms single DB shard | Redis absorbs >99%; rate-limit at CDN per IP; add short-code-level cache at CDN edge |

---

## Key Talking Points for the Interview

1. **The 50:1 read:write ratio** is why caching is the dominant design choice here — not sharding.
2. **Random vs counter codes** is a security discussion, not just engineering.
3. **Eventual consistency is acceptable** for the redirect path (stale cache serves the old URL for up to 24h if someone updates a link), but **read-your-writes** matters right after shortening.
4. **Analytics decoupling** via Kafka demonstrates knowledge of write amplification and async processing.
