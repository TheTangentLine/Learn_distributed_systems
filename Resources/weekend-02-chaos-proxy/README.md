# Weekend 2 — Chaos Engineering Proxy

**Week 2 weekend project.** A TCP chaos proxy that sits between gRPC clients and the chat server, injecting configurable latency and packet drops to observe failure behaviour.

## Concepts demonstrated

- Chaos engineering: deliberately injecting failures to find weaknesses
- Network partitions and their effect on distributed applications
- gRPC timeout handling on the client side
- Fallacy #1 (network is reliable) and #2 (latency is zero) made concrete

## Prerequisites

The Week 1 chat server must be running. Start it first:

```bash
cd Resources/weekend-01-grpc-chat
go run server.go
```

## Setup

```bash
cd Resources/weekend-02-chaos-proxy
go mod tidy
```

## Run the chaos experiment

Open four terminal windows:

```bash
# Terminal 1 — chat server (from weekend-01)
cd Resources/weekend-01-grpc-chat && go run server.go

# Terminal 2 — chaos proxy: 500ms latency, 20% packet drop, forwarding to server
cd Resources/weekend-02-chaos-proxy
go run chaos_proxy.go 50052 localhost:50051 500 20

# Terminal 3 — client through chaos proxy
cd Resources/weekend-01-grpc-chat && go run client.go Alice localhost:50052

# Terminal 4 — client through chaos proxy
cd Resources/weekend-01-grpc-chat && go run client.go Bob localhost:50052
```

## CLI flags

```
chaos_proxy <listen-port> <target-host:port> <latency-ms> [drop-percent]
```

Examples:
```bash
# 0ms latency, 0% drop (baseline — should behave identically to direct)
go run chaos_proxy.go 50052 localhost:50051 0

# 2000ms latency, 0% drop (simulates slow cross-region link)
go run chaos_proxy.go 50052 localhost:50051 2000

# 500ms latency, 50% drop (aggressive chaos)
go run chaos_proxy.go 50052 localhost:50051 500 50
```

## What to observe

1. Messages arrive late (latency).
2. Some messages never arrive (drops). Raise drop to 50% and count how many messages are lost.
3. The client from Day 5 has a 2-second timeout on `SendMessage`. When the proxy drops a packet, does the client return an error or hang?
4. Try connecting one client directly to `:50051` and one through the proxy. The direct client sees messages immediately; the proxied client sees them delayed.

## Experiment checklist

- [ ] Confirm baseline works (0ms, 0%)
- [ ] Observe 500ms latency in message delivery timestamps
- [ ] Observe dropped messages at 30% drop rate
- [ ] Add a retry loop in client.go — does retrying help or make things worse?
- [ ] Raise drop to 100% — what happens to the gRPC stream?
