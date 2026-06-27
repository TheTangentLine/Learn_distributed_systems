# Weekend 8 — Distributed Key-Value Store

**Week 8 final project.** A self-contained distributed KV store running as three Docker containers on a shared network. It implements consistent hashing, N=3 W=2 R=2 quorum replication, vector clocks, and read repair.

## Concepts demonstrated

- Consistent hashing ring to route keys to N replica nodes
- Quorum writes (W=2) and reads (R=2) — survives 1 node failure
- Vector clocks on each value for conflict detection
- Read repair: stale replicas are healed asynchronously during a GET
- Inter-node forwarding via internal HTTP endpoints
- Docker Compose for simulating a multi-node cluster locally

## Architecture

```
Client
  │ PUT /key/name value="Kha"
  ▼
Any node (acts as coordinator)
  │ ring.GetN("name", 3) → [node1, node2, node3]
  ├── PUT /internal/set/name → node1  ─┐
  ├── PUT /internal/set/name → node2  ─┤ wait for W=2 acks
  └── PUT /internal/set/name → node3  ─┘
  │ 2 acks received → 200 OK
  ▼
Client
```

## Setup

```bash
cd Resources/weekend-08-kv-store
docker compose build
docker compose up -d
docker compose ps   # verify all 3 nodes are Running
```

## Basic operations

```bash
# Write
curl -X PUT localhost:8081/key/city \
  -H "Content-Type: application/json" \
  -d '{"value":"Hanoi"}'

# Read (try all three nodes — all should return the same value)
curl localhost:8081/key/city
curl localhost:8082/key/city
curl localhost:8083/key/city

# Delete
curl -X DELETE localhost:8081/key/city
```

## Failure simulation

```bash
# Stop node3 — simulates a crashed node
docker compose stop node3

# Write still succeeds (W=2, only 2 nodes needed)
curl -X PUT localhost:8081/key/city \
  -H "Content-Type: application/json" \
  -d '{"value":"Hue"}'

# Read still succeeds (R=2, only 2 nodes needed)
curl localhost:8081/key/city

# Restart node3 — it is now stale (still has "Hanoi")
docker compose start node3

# Direct read from node3's internal endpoint — stale!
curl localhost:8083/internal/get/city

# Trigger read repair by doing a normal GET through the coordinator
curl localhost:8081/key/city

# node3 is now healed
curl localhost:8083/internal/get/city
```

## Run the full test script

```bash
chmod +x test.sh
./test.sh
```

## File overview

| File | Purpose |
|------|---------|
| `main.go` | All node logic: ring, store, coordinator, handlers, read repair |
| `Dockerfile` | Multi-stage build: compile → minimal alpine image |
| `docker-compose.yml` | 3-node cluster with shared `kvnet` network |
| `test.sh` | Automated curl test covering all operations and failure modes |
