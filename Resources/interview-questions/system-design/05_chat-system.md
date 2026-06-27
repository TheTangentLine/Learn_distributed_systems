# System Design: Chat System

> Classic difficulty: Hard. Tests connection management, message ordering, fan-out, and presence detection.

---

## Step 1 — Clarify Requirements

**Functional:**
- 1-to-1 direct messaging (required)
- Group chats of up to 500 members (required)
- Message delivery acknowledgement (sent / delivered / read)
- Online presence ("Alice is online", "last seen 2 minutes ago")
- Message history — load the last 50 messages when opening a chat
- Push notifications when the recipient is offline (out of scope for deep-dive, reference `06_notification-system.md`)

**Non-functional:**
- Message delivery latency < 100ms p99 (in-app)
- Exactly-once delivery semantics (no duplicate messages shown)
- Eventual consistency for message ordering (causal order; not global total order)
- Scale: 500M DAU, 60B messages/day ≈ 700,000 messages/sec

**Out of scope:** end-to-end encryption, media storage, message translation

---

## Step 2 — Estimate Scale

```
Messages/sec:  700,000/sec
Average message: 200 bytes (text + metadata)
Write throughput: 700,000 × 200 bytes = 140 MB/sec

Storage:
  60B msgs/day × 200 bytes = 12 TB/day
  Retain for 5 years: 12 TB × 365 × 5 ≈ 21 PB → tiered storage (hot vs cold)

Connections:
  500M DAU; assume 50% online simultaneously = 250M concurrent connections
  TCP + WebSocket: ~50 KB per idle connection
  250M × 50 KB = 12.5 TB of RAM just for connections
  → Need ~62,500 connection servers at 200K connections each
  → In practice: 1,000 connection servers at 250K connections (2GB per server for connections)

Fan-out for group chats:
  500 members × 700K msgs/sec = 350M deliveries/sec peak
```

---

## Step 3 — High-Level Design

```
┌─────────┐  WebSocket  ┌──────────────────┐
│  Client │◄──────────►│ Connection Server │
└─────────┘             │  (stateful)       │
                        └────────┬──────────┘
                                 │ publishes message
                                 ▼
                        ┌──────────────────┐
                        │  Message Broker  │
                        │  (Kafka)         │
                        └────────┬─────────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
             ┌──────────┐ ┌──────────┐ ┌──────────┐
             │Delivery  │ │ Storage  │ │Presence  │
             │ Service  │ │ Service  │ │ Service  │
             └────┬─────┘ └────┬─────┘ └──────────┘
                  │            │
                  ▼            ▼
          Connection       Message DB
          Server (fan-out) (Cassandra)
```

**Key insight:** connection servers are stateful (they hold open WebSocket connections). All other services are stateless and can scale horizontally.

---

## Step 4 — Deep Dive

### Transport: WebSocket vs Long-Polling vs SSE

| Transport | Latency | Server cost | Direction | Use when |
|-----------|---------|-------------|-----------|---------|
| **WebSocket** | Lowest (~5ms) | High (persistent connection) | Bidirectional | Preferred for chat; real-time |
| Long-polling | ~500ms | Medium (frequent reconnects) | Client-pull | Fallback when WebSocket is blocked |
| SSE | ~50ms | Low (server-push only) | Server→Client | Read-only feeds; notifications |

**WebSocket connection lifecycle:**
```
Client → HTTP Upgrade request → Connection Server
Connection Server → 101 Switching Protocols → Client
[Persistent full-duplex TCP connection maintained]
Connection Server registers: user_id → server_id in Redis
```

### Message Storage Schema (Cassandra)

Cassandra is ideal: write-heavy (append-only), time-ordered reads per conversation, no complex joins.

```
Table: messages
  partition key:  chat_id           ← all messages for a chat on same shard
  clustering key: message_id DESC   ← sorted newest-first
  columns:        sender_id, content, type, status, created_at

Table: chat_members
  partition key:  chat_id
  clustering key: user_id

Table: user_chats
  partition key:  user_id
  clustering key: last_message_at DESC  ← inbox: sorted by most recent activity
```

**Message IDs:** use Snowflake IDs — globally unique, time-ordered, no coordination needed. The time-ordering property makes them natural clustering keys.

### Message Ordering

Problem: sender A sends M1, then M2. Due to network jitter, M2 arrives at the server first. M2 gets `message_id = 1001`, M1 gets `message_id = 1002`. The chat history now shows M2 before M1.

**Solution: Lamport timestamps + client-side sequence numbers**

```
Client A sends:
  { chat_id, content, client_seq: 42, sender_id: A }

Server assigns:
  message_id = snowflake()
  lamport_ts = max(server_lamport, client_seq) + 1  ← reference weekend-03-lamport-chat

Client B renders messages sorted by (lamport_ts, message_id)
```

For causal ordering within a conversation, Lamport timestamps are sufficient. For multi-device sync (concurrent messages from the same user), Vector Clocks detect conflicts.

### Delivery to Recipients (Fan-out)

**For 1-to-1 messages:**
```
Connection Server A receives message from Alice
  │
  ├── 1. Write to Cassandra (durable)
  ├── 2. Lookup: where is Bob's WebSocket connection? (Redis: user_id → server_id)
  │
  ├── Bob is online on Server X:
  │     Publish to Redis Pub/Sub channel `server:X`
  │     Server X pushes message to Bob's WebSocket
  │
  └── Bob is offline:
        Push notification via FCM/APNs (see 06_notification-system.md)
```

**For group chats (up to 500 members):**
```
Connection Server receives message
  │
  └── Publish to Kafka topic `group:{chat_id}`
        │
        └── Delivery Service consumes:
              For each member in group:
                if online → find their connection server → publish to server's Redis channel
                if offline → enqueue push notification
```

Kafka decouples the fan-out: the connection server finishes in milliseconds; the Delivery Service handles the 500-member fan-out asynchronously.

### Exactly-Once Delivery

Messages can be duplicated by retries. To deduplicate at the recipient:

```go
// Client maintains a seen set per chat
type MessageCache struct {
    seen map[string]bool  // message_id → delivered
}

func (c *MessageCache) Deliver(msg Message) {
    if c.seen[msg.ID] {
        return  // duplicate, drop silently
    }
    c.seen[msg.ID] = true
    display(msg)
}
```

Server-side: deduplicate on write using Cassandra's `IF NOT EXISTS` (lightweight transactions):

```cql
INSERT INTO messages (chat_id, message_id, sender_id, content)
VALUES (?, ?, ?, ?)
IF NOT EXISTS;
```

### Presence Detection

**Heartbeat approach:**
```
Client sends: { type: "heartbeat", user_id } every 5 seconds
Connection Server:
  SET presence:user_id "online" EX 15   ← Redis TTL = 3 × heartbeat interval

Other clients query:
  GET presence:user_id
  → "online" or (nil → "offline, last seen = ...")
```

**Last seen timestamp:** update `user.last_seen` in the DB when the connection closes (WebSocket `onclose` event) or when the heartbeat TTL expires.

### Catch-up on Reconnect

When a client reconnects after going offline:
```
Client sends: { type: "reconnect", user_id, last_seen_message_id }
Server queries:
  SELECT * FROM messages WHERE chat_id IN (user's chats) AND message_id > last_seen_message_id
  → return missed messages in batches of 50
```

---

## Step 5 — Failure Modes

| Component | Failure | Impact | Mitigation |
|-----------|---------|--------|-----------|
| Connection server crashes | All clients on that server lose their WebSocket connection | Message delivery interrupted | Client auto-reconnects (exponential back-off); missed messages fetched via catch-up query |
| Kafka consumer lag | Fan-out delays for group messages | Messages arrive late (> 1 second) | Monitor consumer lag; add more Delivery Service consumers; alert at lag > 10,000 messages |
| Cassandra node down | Writes to affected partition fail | Message loss risk | Replication factor 3; quorum writes (W=2, R=2); retry with idempotency |
| Redis presence cache down | Cannot route to correct connection server | Fan-out fails; messages buffered | Re-route: if Redis lookup fails, broadcast to all connection servers (slightly more network traffic) |
| Hot chat room | One Kafka partition (by chat_id) overwhelmed | Consumer lag on popular channels | Use multiple Kafka partitions per chat; Delivery Service shards consumption by partition |

---

## Key Talking Points for the Interview

1. **Stateful vs stateless:** connection servers are stateful (hold sockets); everything else is stateless. This is the fundamental architecture constraint.
2. **Lamport clocks for ordering** (reference `weekend-03-lamport-chat`): explain the happened-before relationship and why wall-clock time is unreliable.
3. **Kafka for group fan-out** decouples the connection server from slow fan-out work. The connection server ACKs the client in < 10ms; Kafka handles the rest.
4. **Catch-up query** is essential for correctness. Any offline period creates a gap; clients must re-sync on reconnect.
5. **Exactly-once delivery** is achieved by at-least-once delivery + client-side deduplication by message ID.
