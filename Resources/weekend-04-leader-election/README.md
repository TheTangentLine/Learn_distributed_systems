# Weekend 4 — Heartbeat-Based Leader Election

**Week 4 weekend project.** Three Go HTTP servers elect a leader via heartbeat pings. If the leader stops pinging within a configurable window, one of the remaining nodes declares itself the new leader.

## Concepts demonstrated

- Heartbeat-based failure detection (simpler alternative to full Raft)
- Watchdog goroutine pattern: monitor a `lastPing` timestamp
- How randomized timing prevents simultaneous elections
- Split-brain risk and why a single initial leader is bootstrapped

## Setup

```bash
cd Resources/weekend-04-leader-election
go mod tidy
```

## Run — three terminal windows

```bash
# Terminal 1 — initial leader (node1 starts as leader on first boot)
NODE_ID=node1 PORT=8081 PEERS=localhost:8082,localhost:8083 go run node.go

# Terminal 2
NODE_ID=node2 PORT=8082 PEERS=localhost:8081,localhost:8083 go run node.go

# Terminal 3
NODE_ID=node3 PORT=8083 PEERS=localhost:8081,localhost:8082 go run node.go
```

## Test leader election

```bash
# Check current cluster state
curl -s localhost:8081/status | jq
curl -s localhost:8082/status | jq
curl -s localhost:8083/status | jq

# Kill the leader (Ctrl+C in Terminal 1)
# Within ~600ms, one of node2 or node3 will detect the missing pings
# and declare itself leader. Check status again:
curl -s localhost:8082/status | jq
curl -s localhost:8083/status | jq
```

## Status response

```json
{"node_id": "node1", "is_leader": true, "term": 1}
```

## Timing parameters (configurable in node.go)

| Parameter | Default | Meaning |
|-----------|---------|---------|
| Ping interval | 150ms | How often the leader sends pings |
| Watchdog interval | 300ms | How often nodes check for a missing leader |
| Failure threshold | 600ms | How long without a ping before declaring election |

## Comparison to Raft

This is a simplified version — it lacks Raft's term-based voting, log safety, and split-vote handling. It demonstrates the core intuition (heartbeat = aliveness signal; missing heartbeat = election trigger) without the full algorithm complexity covered in Days 18–19.
