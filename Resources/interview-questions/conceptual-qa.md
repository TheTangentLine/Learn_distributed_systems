# Conceptual Q&A — Distributed Systems

50+ questions grouped by week. Each answer is 2–5 sentences. These are model answers optimised for interview conciseness. Study these before working through the system-design problems.

---

## Week 1 — Foundations & Networking

**Q1. What is the defining characteristic that makes a system "distributed"?**

A distributed system is one where components run on separate networked computers and coordinate solely by passing messages, with no shared memory or clock. The defining challenge is that any of those network links or computers can fail independently and at any time, while the rest of the system continues running.

---

**Q2. Name and briefly explain three of the 8 Fallacies of Distributed Computing.**

1. *The network is reliable* — networks drop packets, have transient partitions, and cables get cut. Every call must be retried or tolerated as a potential failure.
2. *Latency is zero* — a cross-datacenter call adds 60–150ms; even intra-datacenter adds 0.5–2ms. Assuming zero latency leads to synchronous designs that fall apart under load.
3. *The network is secure* — traffic between services is observable and spoofable unless encrypted. Mutual TLS between services is not optional in production.

---

**Q3. Why is TCP preferred over UDP for most distributed systems communication?**

TCP provides reliable, ordered delivery via acknowledgements and retransmissions, guaranteeing the receiver sees every byte in sequence. UDP is faster (no handshake, no ACK overhead) but drops packets silently — acceptable for video streaming or DNS but not for RPCs or database replication where every message matters.

---

**Q4. What is gRPC and why might it be preferred over REST/JSON for internal service communication?**

gRPC is a high-performance RPC framework that uses Protocol Buffers for binary serialisation over HTTP/2. It is preferred over REST/JSON for internal services because it is significantly faster (binary vs text, multiplexed streams), generates strongly-typed client/server code from `.proto` definitions (eliminating a class of API contract bugs), and supports streaming natively.

---

**Q5. What is forward and backward compatibility in Protocol Buffers? Why does it matter?**

Forward compatibility means old code can read messages that were written by new code (new fields are ignored). Backward compatibility means new code can read messages written by old code (missing fields take their default values). This matters because in a distributed system you can never atomically redeploy all services simultaneously — old and new versions will always coexist for some window during a rollout, so the wire format must be tolerant of field additions and removals.

---

**Q6. What is the difference between latency and bandwidth? Give a concrete example of a system that is latency-bound vs bandwidth-bound.**

Latency is the time for a single message to travel from sender to receiver. Bandwidth is the total volume of data that can be transferred per unit time. An OLTP database serving millions of small queries is latency-bound — each query touches only a few kilobytes but must complete in < 1ms. A video transcoding pipeline is bandwidth-bound — it must sustain hundreds of MB/s throughput but can tolerate seconds of startup latency.

---

**Q7. What is a TCP three-way handshake and why does it add latency to new connections?**

The three-way handshake establishes a TCP connection: (1) client sends SYN, (2) server responds SYN-ACK, (3) client sends ACK. The data transfer can only begin after step 3. This adds one full round-trip of latency (e.g., 50ms cross-datacenter) before any useful work is done, which is why connection pooling and HTTP/2 multiplexing (which reuses one TCP connection for many requests) are critical optimisations.

---

**Q8. What is serialisation and why does binary serialisation (Protobuf, Avro) outperform JSON?**

Serialisation converts an in-memory data structure into a byte sequence for transmission or storage. Binary formats outperform JSON because they use fewer bytes (no field name repetition, packed integers, no quotes/braces), are faster to encode/decode (no string parsing), and are schema-validated (corrupt data is caught early). A Protobuf message is typically 5–10× smaller and 3–6× faster to encode than the equivalent JSON.

---

**Q9. What is an RPC stub and what problem does it solve?**

An RPC stub is auto-generated client-side code that makes a remote procedure call look like a local function call. Without stubs, every call requires manual HTTP marshalling, error handling, and deserialisation. Stubs (generated from `.proto` or IDL files) abstract the network — the caller writes `userService.GetUser(id)` and the stub handles serialisation, HTTP/2 transport, and deserialisation transparently.

---

**Q10. Why can't you simply add a retry to every network call and call it reliable?**

Naive retry without idempotency leads to duplicate side effects. If a payment request is sent, the network times out (but the server processed it), and the client retries, the payment may be charged twice. Reliable retries require: (1) idempotency keys so the server can detect and deduplicate retries, (2) exponential back-off to avoid retry storms, and (3) a maximum retry budget to prevent infinite loops.

---

## Week 2 — System Models & The Impossible

**Q11. State the CAP Theorem in one sentence and explain what it actually means in practice.**

CAP states that a distributed system can guarantee at most two of: Consistency (every read sees the latest write), Availability (every request gets a non-error response), and Partition Tolerance (the system works despite network partitions). In practice, partitions are unavoidable (network cables fail, routers reboot), so the real choice is between CP (accept downtime during a partition) and AP (accept stale reads during a partition).

---

**Q12. Why is "CA" (Consistent and Available but not Partition-Tolerant) impossible for a geographically distributed system?**

A CA system requires that when a network partition occurs, you still get both a consistent response AND availability. But if the network is partitioned and you demand consistency, you must refuse requests from the isolated partition (losing availability). If you demand availability, you must serve potentially stale data (losing consistency). Since partitions are guaranteed to occur eventually in any WAN deployment, CA can only be achieved on a single node with no network hops.

---

**Q13. What does the PACELC theorem add to CAP?**

PACELC says: during a Partition (P), choose between Availability (A) and Consistency (C). Else (E) — when there is no partition — choose between Latency (L) and Consistency (C). This captures the everyday trade-off that CAP ignores: even when the network is healthy, a strongly consistent system adds latency (coordinator round-trips, quorum waits), while an eventually consistent system is faster. Cassandra is PA/EL (AP system, prefers low latency); Zookeeper is PC/EC (CP system, prefers consistency).

---

**Q14. What is a crash-stop failure model? What assumptions does it make that are often violated?**

In the crash-stop model, a failed node immediately stops executing and never recovers. This simplifies reasoning — once a node is silent, you treat it as dead. The violated assumption is crash-recovery: in practice, nodes crash and restart (OS reboot, JVM OOM recovery), so the "dead" node may come back with stale state and start responding. Systems must handle this with epoch numbers or fencing tokens to reject messages from zombie nodes.

---

**Q15. What is a Byzantine fault? Name a real-world system that must tolerate Byzantine faults.**

A Byzantine fault is when a node actively lies — returning incorrect values, sending different responses to different peers, or behaving arbitrarily rather than simply crashing. Blockchain networks must tolerate Byzantine faults because validators are economically incentivised to cheat. Bitcoin's Proof-of-Work and Ethereum's Proof-of-Stake are Byzantine-fault-tolerant consensus mechanisms. Traditional distributed databases (Raft, Zookeeper) assume non-Byzantine failures and are not safe against a compromised node.

---

**Q16. What is the FLP Impossibility result and what does it mean practically?**

Fischer, Lynch, and Paterson proved that no deterministic asynchronous consensus algorithm can guarantee termination in the presence of even one faulty process. This means you cannot build a perfectly correct, live, and fault-tolerant consensus protocol in a purely asynchronous system. Practically, systems like Raft work around this by using timeouts (introducing a partial synchrony assumption) — they do not guarantee liveness in a fully async model but work reliably in real networks where messages eventually arrive.

---

**Q17. What is "split-brain" and how do modern distributed databases prevent it?**

Split-brain occurs when a network partition creates two isolated groups that both believe they are the authoritative primary and begin accepting writes independently. The result is two divergent states that are difficult to reconcile. Prevention strategies include: quorum-based writes (a primary can only accept writes if it can communicate with a majority of nodes, so a minority partition cannot form a second primary), and STONITH ("Shoot The Other Node In The Head") — one side of the partition is forcibly killed before the other takes over.

---

**Q18. What is "eventual consistency" and what guarantee does it actually provide?**

Eventual consistency guarantees that if no new updates are made to a data item, all replicas will eventually converge to the same value. It does NOT guarantee when convergence happens, that reads return the latest value, or that all replicas are consistent at any given moment. It is useful when availability and low latency are more important than seeing the absolute latest state — for example, a social media like count or a shopping cart.

---

**Q19. Compare linearizability and sequential consistency.**

Linearizability (strong consistency) requires that every operation appears to take effect instantaneously at some point between its invocation and completion, and all observers see a single consistent history. Sequential consistency is weaker: operations appear to execute in some total order that is consistent with the order seen by each individual process, but the global order does not need to respect real-world time. Linearizability is what you get from a single-node database; sequential consistency is what Lamport logical clocks provide.

---

**Q20. What is a failure detector and why is it fundamentally unreliable in asynchronous systems?**

A failure detector monitors whether other nodes are alive, typically by sending periodic heartbeats. In a fully asynchronous system, a slow node and a crashed node look identical — both stop sending heartbeats. The FLP result shows this is a fundamental limitation: a perfect failure detector (accurate + complete) is impossible in async systems. Real systems use timeout-based imperfect detectors and accept that they will sometimes suspect live nodes (false positives), which may trigger unnecessary leader elections.

---

## Week 3 — Time and Order

**Q21. Why can't you use `System.currentTimeMillis()` to order events across different machines?**

Clocks on different machines drift at different rates (quartz oscillators are accurate to ~10 ppm ≈ 1ms/day drift), and can jump backward when NTP corrects a fast clock. If server A's clock is 50ms ahead of server B's, an event on A that happened after an event on B can still get a smaller timestamp, inverting the order. Logical clocks (Lamport, Vector) are used instead because they capture causality, not wall-clock time.

---

**Q22. State the two rules of Lamport clocks.**

1. If event A happens before event B on the same process, then `LC(A) < LC(B)` (the clock is monotonically increasing within a process).
2. If process P sends a message with timestamp T to process Q, then Q sets its clock to `max(Q.clock, T) + 1` before processing the received message.

These rules guarantee: if A → B (A happened-before B), then `LC(A) < LC(B)`. The converse is NOT guaranteed — `LC(A) < LC(B)` does not mean A caused B.

---

**Q23. What do Lamport clocks NOT capture that Vector clocks do?**

Lamport clocks cannot detect concurrent events. If `LC(A) < LC(B)`, you cannot tell whether A caused B or they are concurrent. Vector clocks solve this: with a vector `[t1, t2, ..., tN]` (one counter per process), you can determine that A happened-before B, B happened-before A, or A and B are concurrent (neither's vector dominates the other). Concurrent events represent potential conflicts that may need resolution.

---

**Q24. When you compare two vector clocks V1 and V2, what are the three possible outcomes?**

1. **V1 happened-before V2:** `V1[i] ≤ V2[i]` for all i, and `V1[j] < V2[j]` for at least one j.
2. **V2 happened-before V1:** the symmetric case.
3. **Concurrent (conflict):** neither `V1 ≤ V2` nor `V2 ≤ V1` — there exists i where `V1[i] > V2[i]` and j where `V1[j] < V2[j]`. This means the events happened on parallel branches of execution with no causal link.

---

**Q25. What is the Chandy-Lamport snapshot algorithm used for?**

It captures a consistent global state ("snapshot") of a distributed system without stopping it — like taking a photograph of a running system. It is used for: detecting global properties (deadlock detection, garbage collection), checkpointing for fault recovery, and debugging. It works by having each node record its own state when it initiates or receives a special "marker" message, using FIFO channels to ensure all in-flight messages at the moment of the snapshot are also captured.

---

**Q26. What is clock drift and how does NTP address it?**

Clock drift is the gradual divergence of a computer's clock from real time, caused by imperfect quartz oscillators (±10–50ms/day). NTP (Network Time Protocol) periodically synchronises clocks by contacting a hierarchy of reference servers (atomic clocks → stratum 1 → stratum 2 → your machine). NTP achieves ±1–50ms accuracy depending on network conditions. Crucially, NTP can step the clock backward, which breaks any timestamp-based ordering. Systems like Google Spanner use TrueTime (GPS + atomic clocks) to bound clock uncertainty to ±7ms.

---

**Q27. What is the "happened-before" relationship and why is it a partial order, not a total order?**

The happened-before relation (→) is defined as: event A → event B if (a) A and B are on the same process and A occurred before B, (b) A is the sending and B is the receiving of the same message, or (c) transitively. It is a partial order because not all pairs of events have a defined ordering — events on different processes with no message between them are concurrent, and neither A → B nor B → A holds. A total order would require every pair of events to be comparable, which is impossible without a global clock.

---

**Q28. How can vector clocks be used to detect and resolve write conflicts in a distributed KV store?**

Each key stores its value along with a vector clock. On write, the client increments its own component. On read, the KV store returns all conflicting versions (values with concurrent vector clocks). The client must reconcile them — either via application logic (e.g., merging shopping cart items) or via a policy like Last Write Wins. Amazon's Dynamo uses this pattern: divergent values are returned together and the application resolves the conflict on the next write.

---

## Week 4 — Consensus

**Q29. What is the consensus problem in distributed systems?**

Consensus requires that a set of processes agree on a single value, subject to: (1) Agreement — all non-faulty processes decide the same value, (2) Validity — the decided value was proposed by some process (not invented), (3) Termination — all non-faulty processes eventually decide. Consensus underlies leader election, atomic broadcast, and distributed transactions — it is the fundamental building block of coordination.

---

**Q30. What is the "blocking problem" with Two-Phase Commit (2PC)?**

In 2PC, if the coordinator crashes after sending "Prepare" (phase 1) but before sending "Commit/Abort" (phase 2), all participants are blocked indefinitely — they have voted and cannot commit or abort unilaterally without risking inconsistency. They must wait until the coordinator recovers. This makes 2PC unsuitable for high-availability systems: a coordinator crash creates an outage. Three-Phase Commit (3PC) attempts to address this but introduces its own issues with network partitions.

---

**Q31. Describe the Raft leader election process.**

Each Raft node starts as a Follower and has a randomised election timeout (150–300ms). If a Follower does not hear from a Leader within its timeout, it increments its term, transitions to Candidate, and sends `RequestVote` RPCs to all peers. A Candidate wins if it receives a majority of votes. Voters grant a vote if (a) they haven't voted in this term yet and (b) the candidate's log is at least as up-to-date as their own. Randomised timeouts ensure that in most cases only one Candidate fires at a time, preventing split votes.

---

**Q32. What is Raft's Log Matching Property and why is it important?**

The Log Matching Property states: if two logs have an entry with the same index and term, then all preceding entries in both logs are identical. This is maintained by the Leader including the index and term of the previous log entry in every AppendEntries RPC; Followers reject it if there is a mismatch, and the Leader walks back to find the divergence point and replaces the Follower's conflicting entries. This guarantees that once an entry is committed, no future leader can have a different entry at that index.

---

**Q33. Why is Raft considered easier to understand than Paxos, even though they achieve the same thing?**

Raft was explicitly designed for understandability by decomposing consensus into three relatively independent sub-problems: leader election, log replication, and safety. Raft requires a strong leader (single point of control for log ordering), whereas Paxos allows any node to propose values in parallel, requiring complex conflict resolution. Raft's state machine is simpler: a node is always in one of three states (Follower, Candidate, Leader) and transitions are well-defined. Paxos's original formulation is underspecified for practical log replication, requiring the "Multi-Paxos" extension which is rarely fully described.

---

**Q34. What is the Byzantine Generals Problem and what fraction of traitors can BFT algorithms tolerate?**

The Byzantine Generals Problem asks: can a group of generals (distributed nodes) agree on a battle plan (consensus) when some generals may be traitors (sending conflicting messages)? The answer is that consensus is achievable if and only if fewer than one-third of participants are traitors. Specifically, with `f` Byzantine faults, you need at least `3f + 1` total nodes. This is why blockchain networks have large validator sets — the more nodes, the higher the cost of acquiring the 1/3 needed to compromise consensus.

---

**Q35. What is a distributed transaction and when should you avoid it?**

A distributed transaction atomically coordinates writes across multiple services or databases so that either all writes commit or all roll back. It is implemented via 2PC or a saga pattern. You should avoid it when: (a) services are owned by different teams (coupling their availability together is operationally dangerous), (b) latency is critical (2PC adds multiple round-trips), or (c) the operations are long-running (holding locks for seconds creates contention). Prefer sagas (a sequence of local transactions with compensating actions) for microservice architectures.

---

**Q36. What is a Saga and how does it differ from 2PC?**

A Saga breaks a distributed transaction into a sequence of local transactions, each followed by a compensating transaction that can undo it. If step 3 fails, the saga executes compensating transactions for steps 2 and 1. Unlike 2PC, a saga does not hold locks across services — each step commits locally and independently. The trade-off is that a saga can be in an inconsistent intermediate state (e.g., payment deducted but order not yet confirmed), whereas 2PC is atomically consistent. Sagas are eventually consistent; 2PC is strongly consistent.

---

## Week 5 — Partitioning (Sharding)

**Q37. What is the difference between key-range and hash partitioning?**

Key-range partitioning divides the key space into contiguous ranges (e.g., A–M, N–Z), making range scans fast but risking hot spots if all writes go to one range (e.g., a time-series key). Hash partitioning applies a hash function to the key and assigns the result to a partition, distributing keys uniformly and eliminating hot spots — but range queries require scanning all partitions. Most systems (Cassandra, DynamoDB) hash the partition key for distribution and use a clustering key within the partition for ordering.

---

**Q38. Why does consistent hashing only move `1/N` of keys when a node is added, while modulo hashing moves `(N-1)/N`?**

With modulo hashing (`node = hash(key) % N`), changing N requires rehashing almost every key. With a consistent hash ring, each node owns an arc of the circle. Adding a new node splits one arc in half — only keys in that arc move to the new node. The expected fraction of keys that move is `1/(N+1)` of the total, regardless of how large the ring is. This is a critical property for systems that cannot afford a full reshuffle (caches, distributed KV stores).

---

**Q39. What are virtual nodes in consistent hashing and why are they needed?**

Without virtual nodes, each physical node occupies one position on the hash ring. If one node has a larger arc than others, it handles more keys (uneven load) and when it dies, all its load transfers to exactly one neighbour (hot spot). Virtual nodes assign each physical node multiple positions on the ring (e.g., 150 virtual nodes per physical node). This spreads each physical node's load across many arcs, achieving statistical uniformity. When a physical node is removed, its virtual nodes' keys spread across many neighbours rather than flooding one.

---

**Q40. What is a "hot spot" in a partitioned database and how do you mitigate it?**

A hot spot is when one partition receives a disproportionate share of traffic — for example, a celebrity's user record is read by millions of requests per second while other user records are read rarely. Mitigations: (1) add a random prefix/suffix to the hot key to spread it across multiple partitions (`key_0`, `key_1`, ..., `key_9`), forcing reads to query all variants and merge; (2) replicate the hot data to a dedicated cache layer (Redis); (3) use application-level rate limiting on the hot entity.

---

**Q41. What is secondary index partitioning and what are the two strategies for global vs local indexes?**

A secondary index allows querying by a non-partition-key attribute. With a **local (document-partitioned) index**: each partition has its own index covering only its data — writes are fast (update one partition) but reads require scatter-gather (query all partitions). With a **global (term-partitioned) index**: the index itself is partitioned by term, so a query on a specific value hits only one index partition — reads are fast but writes must update multiple index partitions (can be done asynchronously, introducing eventual consistency). DynamoDB uses local indexes by default.

---

**Q42. What happens to a consistent hash ring when a node fails suddenly?**

When a node fails, the next node clockwise on the ring becomes responsible for the failed node's key range. This creates an immediate hot spot on the successor. With N=3 replication (data on 3 consecutive ring positions), the primary owner's failure promotes the first replica to primary automatically — no data is lost. However, the new primary is serving 2× its normal key range until the failed node is replaced or the ring is rebalanced. Health monitoring and capacity planning must account for this "hot neighbour" effect.

---

## Week 6 — Replication & Consistency

**Q43. Explain the formula `R + W > N` for quorum consistency.**

With `N` replicas, a read quorum of `R` and a write quorum of `W`: a read is guaranteed to return the latest write if and only if `R + W > N`. This is because any `R` nodes chosen for reading must overlap with any `W` nodes that received the latest write — the overlap contains at least one node with the fresh data. Example: N=3, W=2, R=2 → at least one reader has the latest write. Setting W=3, R=1 gives strong consistency but slow writes; W=1, R=3 gives fast writes but slow reads.

---

**Q44. What is a sloppy quorum and when is it useful?**

In a sloppy quorum, if the normal `W` write nodes are unavailable, the write is accepted by other available nodes (not the canonical owners) as a temporary measure. The data is later transferred back to the canonical owners via "hinted handoff" when they recover. This increases write availability (you don't fail writes just because one shard's primary is down) at the cost of consistency — a subsequent read from the canonical quorum might miss the write temporarily. Amazon Dynamo uses sloppy quorums for high availability.

---

**Q45. What is "read-after-write" consistency and why is it hard in a replicated system?**

Read-after-write (also called read-your-writes) consistency guarantees that after you write a value, any subsequent read by the same client returns that value or a later one. In a replicated system, a write might go to the primary but a subsequent read might hit a replica that hasn't yet received the replication. Solutions: route the client's own reads to the primary for a brief window after a write; include a "consistent read token" (e.g., a timestamp or LSN) in write responses that the client passes on subsequent reads, allowing the system to wait for replicas to catch up.

---

**Q46. What is Last Write Wins (LWW) and what data can it silently lose?**

LWW resolves write conflicts by keeping the write with the largest timestamp and discarding older ones. It can silently lose concurrent writes: if server A writes `x=1` at t=10 and server B writes `x=2` at t=9 (concurrent writes during a partition), LWW keeps `x=1` even though both writes were legitimate. Any user who made the `x=2` write will see their write silently discarded with no error. LWW is acceptable when overwrites are semantically correct (e.g., a last-known GPS location) but dangerous for additive data (e.g., bank balance).

---

**Q47. What is a CRDT (Conflict-free Replicated Data Type) and give two examples.**

A CRDT is a data structure that can be replicated across nodes and merged without coordination, guaranteeing that all replicas converge to the same state automatically. Examples: (1) **G-Counter** (grow-only counter): each node has its own counter slot; the value is the sum of all slots; merge = take the max of each slot. Two increments on different nodes never conflict. (2) **LWW-Element Set** (last-write-wins set): elements have timestamps; add and remove operations are ordered by timestamp. CRDTs are used in Riak, Redis (for hyperloglog), and collaborative editing tools.

---

**Q48. What is replication lag and what consistency anomalies does it cause?**

Replication lag is the delay between a write being committed on the primary and being applied on replicas. It causes: (1) **Monotonic read violations** — if a user queries two different replicas in sequence, they might see a newer value then an older one (time appears to go backward). Fix: always route a user's reads to the same replica. (2) **Read-your-writes violations** — described in Q45. (3) **Causality violations** — user A comments on a photo that user B hasn't seen yet (because the photo's write is still replicating). Fix: causal consistency with vector clocks or version vectors.

---

**Q49. Compare Saga vs 2PC for distributed transactions.**

| | 2PC | Saga |
|--|------|------|
| Atomicity | Atomic (all-or-nothing) | Eventual (compensating transactions) |
| Consistency | Strong | Eventual |
| Latency | High (multiple round-trips, blocking) | Low (local transactions) |
| Availability | Low (coordinator failure blocks) | High (no central coordinator) |
| Coupling | Tight (all participants must be available) | Loose (each step is independent) |
| Use case | Single database, short transactions | Microservices, long-running workflows |

---

## Week 7 — Patterns & Messaging

**Q50. What is the difference between a message queue (RabbitMQ) and a log-based message broker (Kafka)?**

A message queue delivers each message to one consumer and deletes it after acknowledgement — designed for task distribution where each job is processed exactly once. A log-based broker (Kafka) is an append-only log; messages are retained for a configurable time and each consumer group tracks its own offset. Multiple consumers can read the same messages independently, and consumers can replay from any point. Queues are better for work distribution; logs are better for event streaming, audit trails, and multiple subscribers.

---

**Q51. What is idempotency and why is it critical for distributed systems?**

Idempotency means that executing an operation multiple times produces the same result as executing it once. It is critical because distributed systems use at-least-once delivery — any operation may be retried on timeout or failure. A non-idempotent operation (e.g., `INSERT payment VALUES ...`) retried twice will charge the customer twice. An idempotent version uses `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` — the second execution is a no-op. Every state-modifying API endpoint should accept an idempotency key from the client.

---

**Q52. What is a circuit breaker and what three states does it have?**

A circuit breaker is a proxy that monitors calls to a remote service and stops calling it when it is clearly failing, giving it time to recover. Three states: (1) **Closed** — calls pass through normally; failure rate is tracked. (2) **Open** — failure rate exceeded the threshold; all calls fail immediately without hitting the downstream service (fail-fast). (3) **Half-open** — after a cooldown period, a small number of test calls are allowed; if they succeed, the breaker closes; if they fail, it opens again. This prevents cascading failures.

---

**Q53. What is a Bloom filter and what is its key property?**

A Bloom filter is a probabilistic data structure that answers "is this element in the set?" in O(1) time and O(1) space per element. Its key property is that it has **no false negatives** (if the filter says "not in set", it is definitely not in the set) but may have **false positives** (if it says "in set", the element might not actually be there). This makes it useful as a fast pre-filter: check the Bloom filter first; if negative, skip the expensive DB lookup entirely. Used in Cassandra (per-SSTable Bloom filters) and CDNs (check if a URL is cached before hitting origin).

---

**Q54. How does the Gossip protocol achieve eventual dissemination without a central coordinator?**

In the Gossip (epidemic) protocol, each node periodically selects a random set of peers (fan-out = 3) and shares its latest state. Recipients merge the received state with their own and in turn gossip to other random peers. With fan-out F in a cluster of N nodes, the information reaches all nodes in O(log N / log F) rounds. There is no central coordinator — the protocol is decentralised and resilient to node failures. Used in Cassandra (membership gossip), DynamoDB, and Consul for cluster state propagation.

---

**Q55. What is backpressure in a messaging system and why does it matter?**

Backpressure is a mechanism for the consumer to signal to the producer to slow down when the consumer cannot keep up. Without backpressure, a fast producer overwhelms a slow consumer — the queue grows unboundedly, eventually causing the broker (or consumer) to run out of memory and crash. With backpressure, the producer is throttled (blocking or slowing the ingestion rate) so the system processes only what it can handle. gRPC streams support backpressure natively via HTTP/2 flow control; Kafka consumers express backpressure by not committing offsets, causing the producer to pause.

---

## Week 8 — MapReduce & System Design

**Q56. Explain the MapReduce programming model.**

MapReduce processes large datasets in two phases. The **Map** phase applies a function to each record in parallel across many nodes, emitting `(key, value)` pairs. The shuffle step groups all values by key (sorting and partitioning). The **Reduce** phase applies an aggregation function to all values with the same key, producing the final output. Example: counting word frequencies — Map emits `(word, 1)` for each word; Reduce sums all 1s for each word. The strength is that both phases parallelise trivially across hundreds of nodes with no coordination.

---

**Q57. What problem does the Google File System (GFS) solve that a single NFS mount cannot?**

GFS is designed for large files (multi-GB) with append-heavy workloads across thousands of commodity nodes. It uses a single master (for metadata) and many chunk servers (each storing 64MB file chunks, replicated 3×). Single NFS cannot scale beyond one server's capacity or bandwidth, lacks automatic replication across commodity hardware, and cannot handle concurrent appends from many clients. GFS relaxes POSIX consistency (concurrent appends may produce out-of-order or duplicate data) to achieve high throughput at scale.

---

**Q58. When designing a system, how do you decide between SQL and NoSQL?**

| Factor | Lean SQL | Lean NoSQL |
|--------|----------|-----------|
| Data model | Relational, complex joins | Document, key-value, wide column, graph |
| Schema | Well-defined, evolves slowly | Flexible, evolves rapidly |
| Transactions | ACID required | Eventual consistency acceptable |
| Scale | Vertical + moderate horizontal | Massive horizontal |
| Query patterns | Ad-hoc queries, aggregations | Known access patterns, low-latency lookups |

In system design interviews: use SQL for anything with complex relationships (orders + line items + payments); use Cassandra/DynamoDB for time-series or user-activity data with predictable access patterns.

---

**Q59. What is the difference between horizontal and vertical scaling, and what are the limits of each?**

Vertical scaling (scale-up) adds more CPU/RAM/disk to a single machine. It is simpler (no distributed systems complexity) but has a hard ceiling — the largest available instance — and creates a single point of failure. Horizontal scaling (scale-out) adds more machines. It can scale indefinitely in theory but requires the application to be stateless (or use consistent hashing / sharding for stateful services), introduces distributed systems challenges (CAP, consistency, network overhead), and increases operational complexity.

---

**Q60. Name three things that differentiate a production-grade distributed key-value store from a weekend project.**

1. **Consistent hashing with virtual nodes** — not modulo hashing — so that adding/removing nodes doesn't cause a full reshuffle and load distributes evenly.
2. **Quorum reads and writes (R + W > N)** — not a single write with no replication — so that the system survives node failures without data loss and without manual intervention.
3. **Read repair and anti-entropy** — background processes that detect and fix inconsistencies between replicas caused by network partitions or crash-recovery, ensuring eventual convergence without operator action.
