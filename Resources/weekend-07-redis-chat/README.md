# Weekend 7 — Redis Pub/Sub Chat Decoupling

**Week 7 weekend project.** Decouples the Week 1 chat server by introducing Redis Pub/Sub as a message bus between the receiving layer (gRPC) and the broadcasting layer (Redis subscriber goroutine).

## Concepts demonstrated

- Message broker as a decoupling layer between producers and consumers
- Redis Pub/Sub: fire-and-forget channel messaging
- Why this is better than direct coupling (broadcast logic is separate from receive logic)
- Why this is still not Kafka: missed messages when subscriber is down

## Architecture

```
Client A (gRPC SendMessage)
        │
        ▼ publishes to Redis channel "chat:room:1"
   Redis Pub/Sub
        │
        ▼ subscriber goroutine in server.go
   gRPC Subscribe streams (broadcast to all connected clients)
        │
        ├── Client A stream
        ├── Client B stream
        └── Client C stream
```

The gRPC server now has two responsibilities kept in separate goroutines:

1. **Receive path**: `SendMessage` RPC → publish to Redis.
2. **Broadcast path**: a Redis subscriber goroutine → fan-out to all open gRPC streams.

## Prerequisites

```bash
# Start Redis
docker run -d -p 6379:6379 redis:7

# Install protoc plugins if not already done
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Setup

```bash
cd Resources/weekend-07-redis-chat
go mod tidy

protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/service.proto
```

## Run

```bash
# Terminal 1 — server
go run server.go

# Terminal 2, 3, 4 — clients (identical to weekend-01)
go run client.go Alice
go run client.go Bob
go run client.go Charlie
```

## Observe the decoupling

1. Stop the server, wait 5 seconds, restart it. Reconnect clients. Redis discards the messages sent while the server was down — this is the **Redis Pub/Sub limitation** (no persistence).
2. Open a second server instance on port 50052 (`go run server.go --port 50052`). Both servers subscribe to the same Redis channel, so messages sent to either server appear on clients connected to both. This is the **horizontal scaling benefit** of the broker pattern.

## Comparison to weekend-01

| Aspect | weekend-01 | weekend-07 |
|--------|-----------|-----------|
| Message path | Client → server → direct fan-out | Client → server → Redis → fan-out goroutine |
| Multi-server | Impossible (streams only in one process) | Works: both servers subscribe to the same Redis channel |
| Missed messages | N/A | Lost if server is down when message published |
| Persistence | None | None (use Kafka/Redis Streams for persistence) |
