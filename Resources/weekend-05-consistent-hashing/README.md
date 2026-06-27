# Weekend 5 — Consistent Hashing Ring

**Week 5 weekend project.** A complete consistent hashing ring implementation in Go, with virtual nodes, key routing, rebalancing measurement, and hot-spot mitigation.

## Concepts demonstrated

- Consistent hash ring: keys walk clockwise to the nearest node
- Virtual nodes (vnodes): each physical node appears V times for even distribution
- `1/N` key movement on node add/remove (vs almost-everything moves with `hash(key) % N`)
- Hot-spot mitigation: sharding a celebrity key into N buckets

## Setup

```bash
cd Resources/weekend-05-consistent-hashing
go mod tidy
go run ring.go main.go
```

No external dependencies required.

## What the demo shows

```
=== Initial routing (3 nodes, vnodes=150) ===
  user:1               → NodeB
  ...

=== Adding NodeD: 248/1000 keys moved (24.8%)
    Expected ~25.0% (1/N = 1/4)

=== Distribution after NodeD added ===
  NodeA    : 251 keys (25.1%)
  NodeB    : 247 keys (24.7%)
  NodeC    : 252 keys (25.2%)
  NodeD    : 250 keys (25.0%)

=== Hot spot: all 1000 requests hit key "celebrity:taylor-swift" ===
  NodeC    : 1000 requests    ← all go to one node

=== Hot spot mitigated with 50 buckets ===
  NodeA    : 243 requests
  NodeB    : 258 requests
  NodeC    : 257 requests
  NodeD    : 242 requests    ← spread evenly
```

## Experiment ideas

1. Change `vnodes` from 150 to 1 — observe how uneven the distribution becomes.
2. Change `vnodes` to 500 — does distribution improve further?
3. Try removing a node instead of adding one — verify the same ~25% key movement.
4. Increase `totalKeys` to 100,000 — does the movement percentage stabilise closer to 25%?
