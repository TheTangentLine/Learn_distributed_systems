# Weekend 3 — Lamport Clock Chat

**Week 3 weekend project.** Extends the Week 1 gRPC chat server with Lamport timestamps so messages are displayed in causal order even when they arrive out of order due to network delays.

## Concepts demonstrated

- Lamport clock rules: tick on internal event, increment on send, `max(local, msg) + 1` on receive
- Min-heap buffering of messages by Lamport timestamp (`container/heap`)
- Out-of-order delivery simulation via random per-stream delays in the server broadcast
- Difference between arrival order and causal order

## Prerequisite

This project builds on `weekend-01-grpc-chat`. The proto file here adds a `lamport_ts` field (tag 3) to `Message`. The server and client are rewritten to use it.

## Setup

```bash
cd Resources/weekend-03-lamport-chat
go mod tidy

protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/service.proto
```

## Run

```bash
# Terminal 1 — server (broadcasts with artificial out-of-order delays)
go run server.go

# Terminal 2, 3, 4 — clients
go run client.go Alice
go run client.go Bob
go run client.go Charlie
```

Send messages rapidly. Despite the server introducing random delays on some streams, the client's min-heap ensures messages are displayed in Lamport timestamp order.

## Key difference from weekend-01

| Feature | weekend-01 | weekend-03 |
|---------|-----------|-----------|
| Message ordering | Arrival order (non-deterministic) | Lamport timestamp order (causal) |
| Proto `Message` | `body`, `from` | `body`, `from`, `lamport_ts` |
| Server | Plain broadcast | Stamps with server Lamport clock before broadcast; adds random delay to simulate out-of-order delivery |
| Client | Prints on arrival | Buffers in min-heap; flushes every 50ms in causal order |
