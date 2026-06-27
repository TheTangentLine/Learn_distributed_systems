# System Design: Twitter / X Timeline

> Classic difficulty: Hard. Tests fan-out strategies, eventual consistency, sharding, and the celebrity problem.

---

## Step 1 — Clarify Requirements

**Functional:**
- `POST /tweet` — a user posts a 280-character tweet
- `GET /timeline` — return the 200 most recent tweets from people the user follows (home timeline)
- `GET /profile/{user_id}` — return that user's own tweets (user timeline)
- Like, retweet, follow — mention as secondary but out of scope for deep-dive today

**Non-functional:**
- 300M DAU, 600M MAU
- 600,000 tweet writes/min ≈ 10,000/sec
- Read:write ratio ≈ 100:1 (timelines are read far more than tweets are written)
- Home timeline read p99 < 200ms
- Eventual consistency is acceptable — seeing a tweet 1–2 seconds late is fine

**Out of scope:** search, ad insertion, media storage (but mention CDN for images)

---

## Step 2 — Estimate Scale

```
Tweets/sec:       10,000 writes/sec
Timeline reads:  1,000,000 reads/sec (100× write rate)
Average follows: 200 per user
Max follows:     30,000,000 (celebrities like @BarackObama)

Fan-out writes (worst case):
  10,000 tweets/sec × 200 followers avg = 2,000,000 Redis writes/sec

Tweet storage:
  tweet: 300 bytes (text + metadata)
  10,000/sec × 86,400 sec/day = 864M tweets/day
  864M × 300 bytes ≈ 260 GB/day → needs a distributed store

Timeline cache per user:
  200 tweets × 8 bytes (tweetID) = 1,600 bytes per user
  300M users × 1.6 KB = 480 GB total → needs a Redis Cluster (multiple shards)
```

---

## Step 3 — High-Level Design

```
                     ┌─────────────┐
POST /tweet ──────▶  │ Tweet Service│
                     │             │──▶ Tweet DB (writes)
                     │             │──▶ Fan-out Service ──▶ Redis Timeline Cache
                     └─────────────┘

GET /timeline ──────▶ Timeline Service ──▶ Redis Cache (L1)
                                              │ miss
                                              ▼
                                          Tweet DB (L2)
```

**Tweet DB:** Cassandra or DynamoDB — append-only, high write throughput, easy time-ordered queries by `(user_id, tweet_id)`.

**Timeline cache:** Redis sorted set per user. Score = tweetID (snowflake ID encodes timestamp → natural sort order).

---

## Step 4 — Deep Dive

### Strategy A: Fan-out on Write (Push Model)

When user A tweets, **precompute** and insert the tweet into the timeline cache of every follower.

```
User A tweets
  │
  ▼
Tweet written to DB
  │
  ▼
Fan-out Service reads follower list of A (could be 200 entries)
  │
  ├── For each follower f:
  │     ZADD timeline:f  <tweetID_score>  <tweetID>
  │     ZREMRANGEBYRANK timeline:f 0 -201  (keep only 200 most recent)
  │
  └── Done: reads are now O(1) (just read from Redis)
```

**Pros:**
- Timeline read is instant — just `ZRANGE timeline:user_id -200 -1` from Redis
- Scales read path to millions of reads/sec with many Redis replicas

**Cons:**
- Write amplification: a celebrity with 30M followers triggers 30M Redis writes per tweet
- High write latency for the tweeter (they don't know when all followers are updated)
- Storage: timeline cache for all 300M users = 480 GB

---

### Strategy B: Fan-out on Read (Pull Model)

When user B requests their timeline, **compute it at read time** by merging the tweet feeds of everyone B follows.

```
User B requests timeline
  │
  ▼
Fetch follower list of B → [user_1, user_2, ..., user_200]
  │
  ├── For each followed user u:
  │     Fetch last 200 tweets by u from tweet DB (or per-user tweet cache)
  │
  └── Merge 200 sorted lists → take top 200 → return
```

**Pros:**
- Zero write amplification — a celebrity tweet costs one DB write
- No timeline cache storage required

**Cons:**
- Read is expensive: 200 DB fetches + merge sort per timeline request
- At 1M reads/sec this is 200M DB queries/sec — completely infeasible at scale

---

### Strategy C: Hybrid (Twitter's Actual Approach)

- **Fan-out on write for normal users** (< 10,000 followers): precompute timelines in Redis
- **Fan-out on read for celebrities** (> 10,000 followers): exclude celebrity tweets from fan-out; inject them at read time

```
GET /timeline for user B
  │
  ├── 1. Read precomputed timeline from Redis (tweets from normal users B follows)
  │
  ├── 2. Check if B follows any celebrities
  │     If yes: fetch last N tweets from each celebrity's tweet cache
  │
  └── 3. Merge and return top 200
```

**Result:** read path stays fast (Redis + a few extra celebrity lookups); write path is not overwhelmed by fan-out.

### Sharding Strategy

**Tweets DB (Cassandra):**
- Partition key: `user_id` → all tweets for a user on the same shard (efficient profile timeline queries)
- Clustering key: `tweet_id` DESC (snowflake, time-ordered) → range scans are fast

**Timeline Cache (Redis Cluster):**
- Key: `timeline:{user_id}` → hash slot = `CRC16(user_id) % 16384` (Redis Cluster default)
- Uniform distribution; no manual sharding needed

**Snowflake Tweet IDs:**
- 64-bit: `41 bits timestamp | 10 bits machine ID | 12 bits sequence`
- Globally unique, time-ordered, no central coordination
- Used as both the primary key and the sorted-set score

### Following Graph

- Store in a graph DB (Neo4j) or a dedicated follower service backed by a wide-column store
- `(follower_id, followee_id, followed_at)` with indexes on both sides
- Fan-out service queries `SELECT follower_id WHERE followee_id = A LIMIT 100 OFFSET x` in batches

---

## Step 5 — Failure Modes

| Component | Failure | Impact | Mitigation |
|-----------|---------|--------|-----------|
| Fan-out service crash mid-fan-out | Some followers don't see the tweet immediately | Eventual inconsistency (acceptable) | Kafka for fan-out jobs; at-least-once delivery; idempotent Redis writes |
| Redis timeline cache goes cold (eviction or restart) | Timeline reads miss cache → hit DB | DB overload | Rebuild cache from DB on miss (lazy); pre-warm on startup for top-N active users |
| Celebrity with 100M followers tweets | 100M fan-out writes in seconds | Fan-out queue depth spikes | Hybrid model: celebrities excluded from fan-out; injected at read time |
| Cassandra node down | Some tweet writes fail or reads slow | Data unavailable for partition | Replication factor 3; quorum writes (W=2, R=2); coordinator retries |
| Timeline service crash | Users cannot load timeline | Service unavailable | Run ≥ 3 instances behind load balancer; health checks |

---

## Key Talking Points for the Interview

1. **The celebrity problem** (also called the "hotspot" or "thundering herd" fan-out) is the central design challenge. Show you know the hybrid solution.
2. **Snowflake IDs** are the standard answer for tweet IDs — time-ordered, globally unique, no central coordinator.
3. **Eventual consistency is a deliberate choice:** seeing a tweet 1 second late is acceptable. Locking followers to ensure immediate delivery would destroy write performance.
4. **Write amplification math:** 10K tweets/sec × 200 followers = 2M Redis writes/sec. This justifies the fan-out-on-read exception for celebrities.
