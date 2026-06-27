# Day 27: Replication Lag

## 1. The Staleness Problem

Async replication is fast but introduces **replication lag**: the replica is behind the primary by some duration. During this window, reads from the replica return stale data.

This creates real UX bugs:

- **Write your own profile, refresh, see old data:** you wrote your new bio on the primary but the page reloads from a replica that hasn't caught up.
- **Read your own tombstone:** you delete an item, then immediately query a replica — the item is still visible.
- **Moving backwards in time:** you read from replica A (5s behind), then from replica B (10s behind) — the second read appears older.

## 2. Consistency Models for Reads

### Read-your-writes consistency (RYW)

After a user writes, they always see their own writes on subsequent reads — even if reads go to replicas.

**Implementation options:**
- For 1 second after any write from a user, route their reads to the primary.
- Track the replication offset of the last write; only serve from a replica that has at least that offset.
- Sticky sessions: always route a user's requests to the same replica.

### Monotonic reads

A user's successive reads never go backwards in time. If they read from replica A first and it showed value V2, they must never later read from replica B and see the older value V1.

**Implementation:** hash the user ID to always read from the same replica.

### Consistent prefix reads

If events A and B are written in that order, any reader must see A before B (not B before A). Important for causally related events (e.g., "question" must appear before "answer").

---

## Hands-on Assignment (Go)

We extend the Day 26 setup to observe staleness and then implement read-your-writes.

### Step 1: Observe the stale read bug

Copy `dist-sys-day26` to `dist-sys-day27`. Keep the async replication delay at 200ms.

Add a `lastWriteTime` per client (we will simulate this via a query param):

```go
http.HandleFunc("/ryw-read", func(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	// In a real system, compare client's last-write timestamp to replica's replication timestamp
	// Here we simulate by routing to primary if within 300ms of write
	writeTimeStr := r.URL.Query().Get("written_at")
	if writeTimeStr != "" {
		writtenAt, _ := time.Parse(time.RFC3339Nano, writeTimeStr)
		if time.Since(writtenAt) < 300*time.Millisecond {
			// Too recent — route to primary to guarantee RYW
			v, _ := primary.Get(key)
			fmt.Fprintf(w, "primary (RYW): %s=%q\n", key, v)
			return
		}
	}
	v, _ := replica.Get(key)
	fmt.Fprintf(w, "replica: %s=%q\n", key, v)
})
```

### Step 2: Run the experiment

```bash
go run main.go
```

```bash
# Step 1: record the time before writing
WRITE_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S.%NZ")

# Step 2: write
curl "localhost:8080/write?key=bio&val=distributed-systems-engineer"

# Step 3: read back immediately (should see fresh data via primary, not stale replica)
curl "localhost:8080/ryw-read?key=bio&written_at=$WRITE_TIME"

# Step 4: read without the RYW hint — shows stale
curl "localhost:8080/read-replica?key=bio"

# Step 5: wait for replication, then replica is fresh too
sleep 0.4
curl "localhost:8080/read-replica?key=bio"
```

### Step 3: Implement monotonic reads

Add a `/monotonic-read` endpoint that returns the replica's current timestamp alongside the value. The client tracks this and only reads from the replica if the replica's timestamp is ≥ the client's last-seen timestamp:

```go
http.HandleFunc("/monotonic-read", func(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	v, _ := replica.Get(key)
	fmt.Fprintf(w, `{"value":"%s","replica_ts":"%s"}`,
		v, time.Now().Format(time.RFC3339Nano))
})
```

_The client-side logic (comparing `replica_ts` values) is left as a thought exercise: how would you implement it in a mobile app that makes two successive reads?_

---

## Review

1. Describe a real-world scenario (outside of the examples above) where replication lag would cause a visible UX bug. How would you fix it without routing all reads to the primary?

2. Is read-your-writes consistency a property of the server or the client? Could a load balancer enforce it without modifying the application?
