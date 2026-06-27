# Distributed Systems — Interview Questions

A complete reference for distributed systems engineering interviews. Use this alongside the 8-week curriculum notes.

---

## The 5-Step Framework

Every system design question should be answered in this order. Never skip a step.

```
1. Clarify requirements    5 min
2. Estimate scale          5 min
3. High-level design      10 min
4. Deep dive              15 min
5. Failure modes           5 min
```

### Step 1 — Clarify requirements (5 min)

Ask before drawing anything:

- **Functional:** what does the system do? What are the core APIs?
- **Non-functional:** what is the scale? Latency SLA? Durability guarantees? Consistency requirement?
- **Out of scope:** what are you explicitly NOT designing? (auth, billing, analytics)

_Tip: write the functional requirements on the board first. The interviewer will correct or extend them. This prevents wasting time designing the wrong system._

### Step 2 — Estimate scale (5 min)

Use round numbers. Show your working.

- **QPS:** reads/sec, writes/sec
- **Storage:** bytes per record × record count = total storage
- **Bandwidth:** bytes per request × QPS = MB/s on the wire
- **Cache size:** what % of reads are hot? How much RAM do you need?

_Tip: the estimates drive every subsequent decision. A write-heavy system needs different choices than a read-heavy one._

### Step 3 — High-level design (10 min)

Draw the major components and their connections:

- Client / CDN / Load Balancer / App servers / Cache / Database
- Label the arrows with the protocol (HTTP, gRPC, Kafka topic, etc.)
- State the primary data flow for the two most important operations

_Tip: keep it to 5–7 boxes. Don't add Kubernetes, sidecars, or monitoring yet — that is noise at this stage._

### Step 4 — Deep dive (15 min)

The interviewer will steer you here. Common deep-dives:

- **Partitioning:** how do you shard the database? Consistent hashing? Key-range?
- **Replication:** single-leader, multi-leader, or leaderless? What are the consistency guarantees?
- **Caching:** what is the cache invalidation strategy? What happens on a cache miss storm?
- **Ordering / consistency:** do you need linearizability or eventual consistency? Why?
- **API design:** exact request/response shapes, pagination, idempotency keys

_Tip: always justify your choices in terms of the CAP/PACELC trade-off. "I chose eventual consistency here because…"_

### Step 5 — Failure modes (5 min)

For the 3 most important components, answer: "What happens if this dies?"

Typical answers involve: retry logic, circuit breakers, fallback reads, graceful degradation.

---

## Red Flags Interviewers Watch For

| Red flag | What it signals |
|----------|----------------|
| Jumping straight to "use Kafka" or "use Redis" | No requirements analysis; solutions looking for problems |
| No capacity math | Cannot reason about scale trade-offs |
| Single point of failure in every component | Does not think about production reliability |
| "We'll just scale horizontally" | Does not know what makes horizontal scaling hard |
| Ignoring consistency requirements | Does not understand CAP; will build systems with silent data loss |
| Never mentioning failure modes | Does not have production operations experience |

---

## Study Order

1. **`conceptual-qa.md`** — review theory Q&A to sharpen your vocabulary
2. **`system-design/01_url-shortener.md`** — start here; it is the simplest and establishes the pattern
3. Work through the remaining system-design files in order

---

## System Design Problems

| File | Problem | Key concepts tested |
|------|---------|-------------------|
| [01_url-shortener.md](system-design/01_url-shortener.md) | URL Shortener | Partitioning, caching, idempotency, code generation |
| [02_rate-limiter.md](system-design/02_rate-limiter.md) | Rate Limiter | Token bucket, sliding window, distributed coordination |
| [03_twitter-timeline.md](system-design/03_twitter-timeline.md) | Twitter / X Timeline | Fan-out, celebrity problem, sharding, eventual consistency |
| [04_distributed-cache.md](system-design/04_distributed-cache.md) | Distributed Cache | Consistent hashing, eviction, thundering herd |
| [05_chat-system.md](system-design/05_chat-system.md) | Chat System | Message ordering, presence, fan-out, connection management |
| [06_notification-system.md](system-design/06_notification-system.md) | Notification System | At-least-once delivery, idempotency, fan-out |

---

## Conceptual Q&A

See **[conceptual-qa.md](conceptual-qa.md)** for 50+ theory questions covering all 8 weeks, grouped by topic:

- Networking & RPC (Week 1)
- CAP, PACELC, Failure Models (Week 2)
- Time, Lamport Clocks, Vector Clocks (Week 3)
- Consensus, Raft, 2PC (Week 4)
- Consistent Hashing, Partitioning (Week 5)
- Replication, Quorums, CRDTs (Week 6)
- Messaging Patterns, Circuit Breakers (Week 7)
- MapReduce, System Design Interviews (Week 8)
