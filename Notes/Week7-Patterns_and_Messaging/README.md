# Week 7 — Patterns & Messaging, Deep Intro

[Back to top README](../../README.md)

## TL;DR

- **What you learn:** the architectural glue of large distributed systems — how services communicate asynchronously through message queues and log brokers, why idempotency is essential, how circuit breakers prevent cascading failures, how gossip propagates cluster state, and how bloom filters answer "have I seen this before?" in O(1).
- **Tools:** Go + Redis Pub/Sub for the weekend project; Docker to run RabbitMQ/Kafka locally.
- **Mental model:** synchronous calls couple caller and callee in time. Async messaging decouples them. The trade-off is complexity: you gain resilience and scale, you lose simplicity and immediate feedback.

---

## Architecture at a glance

```mermaid
flowchart LR
    subgraph sync ["Synchronous (Week 1 style)"]
      SC["Chat Client"] -->|gRPC call, waits| SS["Chat Server"]
    end
    subgraph async ["Asynchronous (Week 7 style)"]
      AP["Publisher\n(Chat Client)"] -->|publish msg| Broker["Message Broker\n(Redis / Kafka)"]
      Broker -->|deliver msg| AS1["Subscriber 1"]
      Broker -->|deliver msg| AS2["Subscriber 2"]
    end
```

With async messaging, the publisher does not wait for subscribers. It fires a message and continues. Subscribers consume at their own pace. The broker absorbs load spikes and decouples failure domains.

---

## Message queues vs. log-based brokers

The two fundamental messaging architectures:

```mermaid
quadrantChart
    title Messaging architecture choice
    x-axis "Ephemeral (consumed once)" --> "Durable (replay)"
    y-axis "Point-to-point" --> "Pub/Sub fanout"
    quadrant-1 "Log broker pub/sub"
    quadrant-2 "Log replay"
    quadrant-3 "Work queue"
    quadrant-4 "Durable queue fanout"
    "RabbitMQ work queue": [0.2, 0.2]
    "RabbitMQ fanout exchange": [0.2, 0.8]
    "AWS SQS": [0.3, 0.2]
    "Redis Pub/Sub": [0.1, 0.85]
    "Kafka consumer group": [0.85, 0.25]
    "Kafka topics multi-group": [0.9, 0.9]
```

### Message queue (RabbitMQ, SQS)

- A message is delivered to **one** consumer and then deleted.
- If the consumer crashes without ACKing, the message is redelivered to another consumer.
- No replay — once consumed, the message is gone.
- Use for: work queues (image processing jobs, email sending), task distribution.

```mermaid
flowchart LR
    P["Producer"] -->|publish| Q[("Queue")]
    Q -->|deliver| C1["Consumer 1\n(processes, ACKs, message deleted)"]
    Q -->|redelivery on crash| C2["Consumer 2"]
```

### Log-based broker (Kafka)

- Messages are appended to a **log** (sequential file). They are not deleted after consumption.
- Multiple consumer groups can each read the full log independently, at different offsets.
- Consumers commit their offset — on crash, they resume from the last committed offset.
- Use for: event streaming, audit logs, CQRS event store, replay for new consumers.

```mermaid
flowchart LR
    P["Producer"] -->|append| Log["Partition log\n[0][1][2][3][4]..."]
    Log -->|"offset 3"| CG1["Consumer Group A\n(analytics)"]
    Log -->|"offset 1"| CG2["Consumer Group B\n(audit)"]
    Log -->|"offset 4"| CG3["Consumer Group C\n(new service)"]
```

### Kafka internals

- **Topic:** a named log, divided into **partitions**.
- **Partition:** an append-only, ordered, immutable sequence of records. Records have an offset.
- **Consumer group:** a set of consumers that together consume all partitions of a topic. Each partition is assigned to exactly one consumer in the group at a time.
- **Offset:** the position in the partition. Consumers commit offsets to Kafka (or their own store) to track progress.
- **Retention:** Kafka keeps records for a configurable time (default: 7 days) or until a size limit. Records are not deleted on consumption.

```mermaid
flowchart LR
    subgraph topic ["Topic: orders (3 partitions)"]
      P0["Partition 0\noffset 0..100"] --> C0["Consumer in group A"]
      P1["Partition 1\noffset 0..80"] --> C1["Consumer in group A"]
      P2["Partition 2\noffset 0..95"] --> C2["Consumer in group A"]
    end
```

**Ordering guarantee:** Kafka preserves order within a partition. To guarantee that all events for a given entity (e.g., all writes to `user:42`) are processed in order, use the entity ID as the partition key.

---

## Idempotency

A request is idempotent if applying it N times has the same effect as applying it once.

### Why it matters

With at-least-once delivery (the default for any retry-enabled system), the same message may be delivered more than once:

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant C as Consumer
    P->>B: Publish "charge $10"
    B->>C: Deliver "charge $10"
    C->>C: Process charge
    Note over C: ACK lost in transit
    B->>C: Redeliver "charge $10" (timeout)
    C->>C: Process charge AGAIN (duplicate!)
```

### Idempotency key pattern

```mermaid
sequenceDiagram
    participant Client
    participant PaymentService
    participant DB
    Client->>PaymentService: POST /charge {amount:10, idempotency_key:"txn-abc-123"}
    PaymentService->>DB: SELECT * FROM charges WHERE key='txn-abc-123'
    alt key not found
      PaymentService->>DB: INSERT charge, key='txn-abc-123'
      PaymentService-->>Client: 200 OK {charged: true}
    else key found
      PaymentService-->>Client: 200 OK {charged: true} (cached response, no re-charge)
    end
```

Store the idempotency key in the same transaction as the business operation. If the key exists, return the original response. This turns at-least-once delivery into effectively-once processing.

**Idempotent HTTP methods:** GET, HEAD, PUT, DELETE are idempotent by definition. POST is not.

---

## Circuit breaker pattern

Prevents a slow or failing dependency from causing your entire service to fail.

### State machine

```mermaid
stateDiagram-v2
    [*] --> Closed
    Closed --> Open : failure rate > threshold\n(e.g. >50% in last 10 calls)
    Open --> HalfOpen : after cool-down period\n(e.g. 30 seconds)
    HalfOpen --> Closed : trial request succeeds
    HalfOpen --> Open : trial request fails
```

**Closed:** requests pass through normally. Failures are counted in a rolling window.

**Open:** requests fail immediately without reaching the dependency. Callers get a fast error instead of a slow timeout. This sheds load from the failing dependency and prevents goroutine/thread pool exhaustion.

**Half-Open:** after the cool-down, one trial request is allowed. If it succeeds, the breaker closes; if it fails, it opens again.

### Why circuit breakers prevent cascading failure

Without a circuit breaker: a slow database causes HTTP handler goroutines to pile up waiting for DB responses. Goroutines exhaust memory; the entire service crashes, taking down other services that depend on it.

With a circuit breaker: once the failure threshold is hit, all subsequent DB calls fail fast. Goroutines are freed immediately. The service stays alive and can serve requests that do not touch the DB.

**Used by:** Netflix Hystrix (retired), resilience4j (Java), `sony/gobreaker` (Go), Istio/Envoy sidecar proxy (Week 7 extra).

---

## Service discovery

In a static world, services have fixed IPs. In Kubernetes or Docker, containers restart with new IPs constantly.

```mermaid
flowchart LR
    ServiceA["Service A"] -->|"resolve: inventory"| DNS["CoreDNS / Consul\n(service registry)"]
    DNS -->|"inventory = 10.0.0.5:8080"| ServiceA
    ServiceA -->|gRPC| ServiceB["Inventory Service\n10.0.0.5:8080"]
    Note["When Inventory restarts\nat 10.0.0.9:8080,\nthe registry updates automatically"]
```

**Client-side discovery:** the caller queries the registry and picks an instance (with load balancing). Used by Netflix Eureka, Consul.

**Server-side discovery:** the caller sends to a load balancer; the load balancer queries the registry. Used by AWS ELB, Kubernetes `Service`.

---

## Bloom filters

A space-efficient probabilistic data structure that answers "is this element in the set?" in O(1) time and O(1) space — with a tunable false positive rate.

```mermaid
flowchart LR
    subgraph bloom ["Bloom filter (m=10 bits, k=3 hash functions)"]
      Item["Insert 'user:42'"]
      H1["hash1('user:42') = 2"] --> B2["bit[2] = 1"]
      H2["hash2('user:42') = 5"] --> B5["bit[5] = 1"]
      H3["hash3('user:42') = 8"] --> B8["bit[8] = 1"]
    end
    Query["Query 'user:99'"]
    Query --> QH1["hash1('user:99') = 2 → bit[2]=1 ✓"]
    Query --> QH2["hash2('user:99') = 3 → bit[3]=0 ✗ DEFINITELY NOT IN SET"]
```

- **No false negatives:** if the filter says "not in set," it is definitely not in the set.
- **Possible false positives:** if the filter says "in set," it might be wrong (the bits were set by other elements). The false positive rate is tunable by increasing `m` (bits) or `k` (hash functions).

**Real-world uses:**
- **Cassandra:** avoids reading from SSTables that cannot contain a key.
- **Web crawlers:** skip URLs already crawled.
- **Databases:** check if a row exists before a slow disk read.
- **CDNs:** determine if a URL is "popular enough" to cache.

---

## Gossip protocols

How cluster members propagate information (node health, ring state, configuration) without a central authority.

### The epidemic model

```mermaid
sequenceDiagram
    participant N1 as Node 1 (knows about Node 5 join)
    participant N2 as Node 2
    participant N3 as Node 3
    N1->>N2: gossip {node5: alive, token=42}
    N2->>N3: gossip {node5: alive, token=42}
    N1->>N3: gossip {node5: alive, token=42}
    Note over N2,N3: All nodes learn about Node 5\nwithout a central server
```

Every node periodically picks a random set of neighbors and exchanges its full state. Infected nodes spread the news. Within `O(log N)` rounds, the entire cluster knows.

### SWIM (Scalable Weakly-consistent Infection-style Membership)

Used by Cassandra, Consul, Serf.

1. Each node sends a periodic PING to a random member.
2. If no ACK is received within a timeout, the node sends an `indirect PING` through K random intermediaries.
3. If still no ACK, the member is marked `suspected`.
4. If suspected for too long, it is marked `dead` and the information is gossiped.

This is probabilistic — a slow node may be falsely declared dead. But the false positive rate decreases with the indirect-ping mechanism.

---

## Mental models

### Async messaging trade-offs

| Property | Sync call | Message queue | Log broker |
|----------|-----------|--------------|------------|
| Coupling | tight (caller waits) | loose (consumer at own pace) | very loose (replay possible) |
| Ordering | in request order | FIFO per queue | per-partition |
| Durability | none (in-memory) | configurable | durable by default |
| Replay | impossible | no | yes |
| Use for | user-facing hot path | background jobs | event sourcing, audit, stream processing |

### Bulkhead pattern

Isolate different workloads into separate thread/goroutine pools. A slow database should not exhaust the goroutine pool serving HTTP requests.

```mermaid
flowchart TB
    HTTP["HTTP handler pool\n(100 goroutines)"]
    DB["DB query pool\n(20 goroutines)"]
    Kafka["Kafka consumer pool\n(10 goroutines)"]
    HTTP -->|bounded| DB
    HTTP -.->|does not share| Kafka
```

If `DB` pool is exhausted, only DB-dependent requests fail. `Kafka` consumer continues independently.

---

## Failure modes

- **Message loss in Redis Pub/Sub:** Redis Pub/Sub is fire-and-forget. If the subscriber is disconnected when a message is published, it is lost. For durability, use Redis Streams or Kafka.
- **Consumer lag spike:** if consumers are slow and producers are fast, the Kafka lag grows. Monitor `consumer_lag` per partition. If it grows unboundedly, add consumer instances (up to the number of partitions).
- **Poison pill message:** a malformed message that causes the consumer to crash on every retry, preventing forward progress. Fix: dead-letter queue (DLQ) — after N retries, move the message to a separate queue for manual inspection.
- **Circuit breaker open storm:** all services open their circuit breakers simultaneously (e.g., database restarts). Implement staggered half-open retries with jitter to avoid thundering herd on recovery.
- **Gossip convergence under churn:** if nodes join and leave faster than gossip converges, the cluster state is always stale. Bound the churn rate in production (Cassandra recommends not adding more than 1 node per hour).

---

## Day-by-day links

- [Day 31 — Async Messaging: RabbitMQ work queues vs. Kafka log brokers](day31_async-messaging.md)
- [Day 32 — Idempotency: exactly-once semantics, idempotency key pattern](day32_idempotency.md)
- [Day 33 — Microservice Patterns: Service Discovery, Circuit Breakers, Bulkheads](day33_microservice-patterns.md)
- [Day 34 — Bloom Filters: probabilistic membership at massive scale](day34_bloom-filters.md)
- [Day 35 — Gossip Protocols: SWIM, epidemic algorithms, cluster membership](day35_gossip-protocols.md)
