# Day 31: Asynchronous Messaging

## 1. Why Async?

Synchronous RPC means the caller waits for the callee. If the callee is slow, overloaded, or down, the caller is blocked. This creates tight coupling: a chain of 10 synchronous services means one slow service degrades all 10.

**Asynchronous messaging** decouples producers from consumers via a message broker. The producer drops a message and moves on. The consumer processes at its own pace.

## 2. Message Queues vs Log-Based Brokers

|  | Message Queue (RabbitMQ, SQS) | Log Broker (Kafka, Pulsar) |
|--|-------------------------------|---------------------------|
| **Storage** | Messages deleted after ACK | Messages retained for a configurable period |
| **Consumers** | Each message goes to one consumer | Any number of consumers can read independently (each tracks its own offset) |
| **Replay** | Not possible | Read from any offset — replay last hour, day, etc. |
| **Ordering** | Per-queue (usually) | Per-partition, strictly ordered |
| **Use case** | Task queues, work distribution | Event streaming, audit logs, fan-out |

### Redis Pub/Sub

Redis Pub/Sub is the simplest example: the publisher sends to a channel, all current subscribers receive a copy. It is **fire and forget** — if a subscriber is disconnected, it misses the message. There is no replay, no ACK, no persistence.

This makes it unsuitable for critical events (payments, inventory updates) but fine for real-time notifications (chat, presence).

---

## Hands-on Assignment (Go)

We implement a Redis publisher and subscriber using `go-redis`, and observe the missed message problem.

### Step 1: Start Redis

```bash
docker run -d -p 6379:6379 redis:7
```

### Step 2: Set up the project

```bash
mkdir dist-sys-day31
cd dist-sys-day31
go mod init day31
go get github.com/redis/go-redis/v9
```

### Step 3: Create `publisher.go`

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	if os.Getenv("ROLE") != "pub" {
		// --- SUBSCRIBER ---
		sub := rdb.Subscribe(ctx, "events")
		ch := sub.Channel()
		fmt.Println("Subscriber waiting for messages...")
		for msg := range ch {
			fmt.Printf("[sub] received: %s\n", msg.Payload)
		}
		return
	}

	// --- PUBLISHER ---
	fmt.Println("Publisher starting in 3s (start subscriber first)...")
	time.Sleep(3 * time.Second)

	for i := 1; i <= 10; i++ {
		payload := fmt.Sprintf("event-%d", i)
		rdb.Publish(ctx, "events", payload)
		fmt.Printf("[pub] sent: %s\n", payload)
		time.Sleep(200 * time.Millisecond)
	}
}
```

### Step 4: Observe missed messages

```bash
# Terminal 1 — start subscriber
go run publisher.go

# Terminal 2 — start publisher (will send 10 messages starting at T+3s)
ROLE=pub go run publisher.go
```

Messages 1–10 should appear in Terminal 1.

Now **stop the subscriber** (Ctrl+C). Start the publisher again:

```bash
ROLE=pub go run publisher.go
```

Then start the subscriber again. You will see **zero messages** — all 10 were sent while the subscriber was down and Redis discarded them.

### Step 5: Why Kafka solves this

In Kafka:

1. Messages are written to a persistent log on disk.
2. Each consumer group has an **offset** — its position in the log.
3. When a consumer restarts, it resumes from its last committed offset.
4. Multiple consumer groups can read the same topic independently.

Redis Pub/Sub has no log, no offset, no persistence. If your application cannot afford to miss events, use a log-based broker.

---

## Review

1. You are building a payment service. Should you use Redis Pub/Sub or a log-based broker like Kafka for "payment initiated" events? Why?

2. A Kafka topic has 6 partitions and 2 consumer instances in the same group. How many partitions does each consumer get? What happens when you add a 3rd consumer instance? A 7th?
