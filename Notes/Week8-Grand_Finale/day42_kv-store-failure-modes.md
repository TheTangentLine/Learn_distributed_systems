# Day 42: KV Store — Failure Modes & Read Repair

## 1. Goals for Today

- Run the three nodes in Docker.
- Simulate node failure and verify W=2 writes still succeed.
- Simulate a stale replica and verify R=2 reads return the correct value.
- Implement **read repair** so the stale replica is healed automatically.

---

## Hands-on Assignment (Go)

### Step 1: Create a `Dockerfile`

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o kvnode main.go

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/kvnode .
EXPOSE 8081
CMD ["/app/kvnode"]
```

### Step 2: Create `docker-compose.yml`

```yaml
services:
  node1:
    build: .
    ports:
      - "8081:8081"
    environment:
      NODE_ADDR: "node1:8081"
      NODE_LIST: "node1:8081,node2:8082,node3:8083"
    networks:
      - kvnet

  node2:
    build: .
    ports:
      - "8082:8082"
    environment:
      NODE_ADDR: "node2:8082"
      NODE_LIST: "node1:8081,node2:8082,node3:8083"
    networks:
      - kvnet

  node3:
    build: .
    ports:
      - "8083:8083"
    environment:
      NODE_ADDR: "node3:8083"
      NODE_LIST: "node1:8081,node2:8082,node3:8083"
    networks:
      - kvnet

networks:
  kvnet:
```

### Step 3: Build and start the cluster

```bash
docker compose build
docker compose up -d
```

Verify all three nodes are running:

```bash
docker compose ps
```

### Step 4: Test basic operations

```bash
# Write via node1
curl -X PUT localhost:8081/key/city -d '{"value":"Hanoi"}' -H "Content-Type: application/json"

# Read via all nodes
curl localhost:8081/key/city
curl localhost:8082/key/city
curl localhost:8083/key/city
```

All three should return `{"value":"Hanoi", ...}`.

### Step 5: Simulate node3 failure

```bash
docker stop dist-sys-day42-node3-1
```

**Test write survives failure (W=2 of N=3):**

```bash
curl -X PUT localhost:8081/key/city -d '{"value":"Hue"}' -H "Content-Type: application/json"
# Should succeed — only 2 acks needed
```

**Test read survives failure (R=2 of N=3):**

```bash
curl localhost:8081/key/city
curl localhost:8082/key/city
# Should return "Hue"
```

### Step 6: Restart node3 — observe staleness

```bash
docker start dist-sys-day42-node3-1
sleep 1

# Read from node3 directly (bypassing coordinator)
curl localhost:8083/internal/get/city
# Returns "Hanoi" — stale! Node3 missed the write to "Hue".
```

### Step 7: Implement read repair

Add read repair to `coordinatorGet` in `main.go`:

```go
func coordinatorGet(key string) (*Entry, error) {
	// ... (existing collection logic) ...

	// Read repair: for any node that returned a value older than `best`,
	// asynchronously send the fresh value
	for _, r := range allResults {
		if r.entry == nil { continue }
		if r.entry.Clock.Compare(best.Clock) < 0 {
			go func(addr string, e *Entry) {
				body, _ := json.Marshal(e)
				url := fmt.Sprintf("http://%s/internal/set/%s", addr, key)
				http.Post(url, "application/json", bytes.NewReader(body))
				log.Printf("[read-repair] healed stale replica at %s for key %s", addr, key)
			}(r.addr, best)
		}
	}

	return best, nil
}
```

_Note: you need to collect all results (not just the best) to implement this. Modify `coordinatorGet` to store all results in a slice._

### Step 8: Verify read repair

```bash
# Trigger a read via the coordinator — this should heal node3
curl localhost:8081/key/city

# Now read node3 directly
curl localhost:8083/internal/get/city
# Should now return "Hue" — healed!
```

### Step 9: Final test script

```bash
#!/bin/bash
echo "=== Final KV store test ==="
BASE=localhost:8081

# Write
curl -s -X PUT $BASE/key/counter -d '{"value":"1"}' -H "Content-Type: application/json" | jq .
echo ""

# Read
echo "Read counter:"
curl -s $BASE/key/counter | jq .

# Concurrent writes
echo "Concurrent writes..."
for i in {1..5}; do
  curl -s -X PUT $BASE/key/counter -d "{\"value\":\"$i\"}" -H "Content-Type: application/json" &
done
wait

echo "Final value of counter:"
curl -s $BASE/key/counter | jq .

echo "Delete counter:"
curl -s -X DELETE $BASE/key/counter | jq .

echo "Read after delete:"
curl -s $BASE/key/counter
echo ""
echo "=== Done ==="
```

Save as `test.sh`, run with `bash test.sh`.

---

## Congratulations

You have built a working distributed key-value store from scratch. Let's take stock of what you applied:

| Concept | Where used |
|---------|-----------|
| TCP / HTTP | Transport layer between nodes |
| Protobuf/JSON | Serialization of Entry structs |
| CAP: AP design | W=2 writes succeed with 1 node down |
| PACELC: EL default | Async replication lag between ring nodes |
| Consistent hashing | `ring.GetN(key, N)` routing |
| Replication (N=3) | Every write sent to 3 nodes |
| Quorum (W=2, R=2) | Durability without requiring all nodes |
| Vector clocks | Conflict detection on concurrent writes |
| Read repair | Self-healing stale replicas |
| Gossip (optional) | Could gossip ring topology changes |

**Your next project:** add Raft-based replication to replace the quorum-based approach, giving you strong consistency instead of eventual consistency.
