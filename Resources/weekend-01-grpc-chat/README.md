# Weekend 1 — gRPC Chat Server

**Week 1 weekend project.** A real-time chat application where multiple clients connect to a central server and exchange messages via gRPC server-streaming.

## Concepts demonstrated

- gRPC service definition with Protocol Buffers
- Bidirectional communication using server-streaming RPC
- Go goroutines for concurrent client handling
- `sync.Mutex` for safe shared state

## Prerequisites

```bash
# Protocol Buffer compiler
brew install protobuf   # macOS
# apt install protobuf-compiler  # Linux

# Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Setup

```bash
cd Resources/weekend-01-grpc-chat
go mod tidy

# Generate gRPC stubs from the proto file
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/service.proto
```

## Run

Open four terminal windows in this directory:

```bash
# Terminal 1 — server
go run server.go

# Terminal 2, 3, 4 — clients (each with a different username)
go run client.go Alice
go run client.go Bob
go run client.go Charlie
```

Type a message in any client terminal and press Enter. It appears in all other clients.

## What to observe

- Disconnect a client with `Ctrl+C` — the server prints `<< X left` and the remaining clients continue working.
- Send messages from multiple clients simultaneously — no messages are lost.
- The `go` keyword in `go handleConnection` (the conceptual equivalent here is `go Subscribe`) is what enables concurrent clients.

## Next step

**Week 2 weekend** (`weekend-02-chaos-proxy`) adds a chaos proxy between clients and this server to observe how the system behaves under latency and packet loss.
