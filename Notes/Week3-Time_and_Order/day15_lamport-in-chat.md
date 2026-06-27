# Day 15: Weekend Project — Lamport Clocks in the Chat App

## Goal

Messages in a distributed chat app can arrive out of order because of variable network latency. A message sent at T=5 may arrive before a message sent at T=3 if the T=5 message took a faster route.

Today you retrofit **Lamport timestamps** into the Week 1 gRPC chat server. The receiver buffers incoming messages and displays them in causal order rather than arrival order.

## Why This Matters

Without ordering: clients see messages in the wrong sequence and conversations look incoherent.

With Lamport ordering: even if message B arrives before message A, if A's timestamp is lower, A is displayed first. Conversations are coherent.

---

## Hands-on Assignment (Go)

### Step 1: Update the proto contract

Add a `lamport_ts` field to `Message` in `proto/service.proto`:

```protobuf
message Message {
  string body      = 1;
  string from      = 2;
  uint64 lamport_ts = 3;
}
```

Regenerate:

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/service.proto
```

### Step 2: Add a Lamport clock to the server

In `server.go`, add a server-side Lamport clock that advances on every `SendMessage`:

```go
type LamportClock struct {
	mu    sync.Mutex
	value uint64
}

func (lc *LamportClock) Receive(msgTS uint64) uint64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if msgTS > lc.value {
		lc.value = msgTS
	}
	lc.value++
	return lc.value
}
```

In `SendMessage`:

```go
func (s *chatServer) SendMessage(_ context.Context, msg *pb.Message) (*pb.Ack, error) {
	ts := s.clock.Receive(msg.LamportTs)
	msg.LamportTs = ts  // stamp with server's clock before broadcast

	s.mu.Lock()
	for _, stream := range s.streams {
		stream.Send(msg)
	}
	s.mu.Unlock()
	return &pb.Ack{Status: "ok"}, nil
}
```

### Step 3: Add a Lamport clock and message buffer to the client

In `client.go`, add a min-heap that buffers messages by Lamport timestamp:

```go
package main

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	pb "grpc-demo/proto"
)

// MessageHeap implements heap.Interface for *pb.Message sorted by LamportTs
type MessageHeap []*pb.Message

func (h MessageHeap) Len() int            { return len(h) }
func (h MessageHeap) Less(i, j int) bool  { return h[i].LamportTs < h[j].LamportTs }
func (h MessageHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *MessageHeap) Push(x interface{}) { *h = append(*h, x.(*pb.Message)) }
func (h *MessageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type OrderedDisplay struct {
	mu     sync.Mutex
	buf    *MessageHeap
	lastTS uint64
}

func NewOrderedDisplay() *OrderedDisplay {
	h := &MessageHeap{}
	heap.Init(h)
	return &OrderedDisplay{buf: h}
}

func (od *OrderedDisplay) Add(msg *pb.Message) {
	od.mu.Lock()
	defer od.mu.Unlock()
	heap.Push(od.buf, msg)
}

// Flush prints messages in order, waiting briefly for late arrivals
func (od *OrderedDisplay) FlushLoop() {
	for {
		time.Sleep(50 * time.Millisecond)
		od.mu.Lock()
		for od.buf.Len() > 0 {
			msg := heap.Pop(od.buf).(*pb.Message)
			fmt.Printf("\r[LC=%d][%s]: %s\n> ", msg.LamportTs, msg.From, msg.Body)
		}
		od.mu.Unlock()
	}
}
```

In the goroutine that receives from the stream:

```go
display := NewOrderedDisplay()
go display.FlushLoop()

go func() {
    for {
        msg, err := stream.Recv()
        if err != nil { return }
        display.Add(msg)
    }
}()
```

### Step 4: Add artificial delay to prove ordering works

In the server's `SendMessage`, add a small random delay before broadcasting to the second subscriber to simulate out-of-order arrival:

```go
for i, stream := range s.streams {
    if i == 1 {
        go func(st pb.ChatService_SubscribeServer) {
            time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
            st.Send(msg)
        }(stream)
    } else {
        stream.Send(msg)
    }
}
```

With the buffer, messages should still appear in correct Lamport order on all clients even though arrival order differs.

### Step 5: Test

Open 3 terminals. Send messages rapidly from clients. Verify the displayed order is consistent with Lamport timestamps rather than raw arrival order.

---

## Your Next Step

You have given the chat app a logical sense of time. Next week we go one layer deeper: how do nodes **agree** on a single value when they can't share memory or a clock? This is the hardest problem in distributed systems — **Consensus**.
