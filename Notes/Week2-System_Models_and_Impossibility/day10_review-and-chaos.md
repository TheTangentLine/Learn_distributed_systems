# Day 10: Review & Chaos Engineering

## 1. Week 2 Review Quiz

Answer these before looking at your notes. Check each answer against Days 6–9.

**Q1.** A database cluster refuses all write requests when the network partitions, but reads still work from the remaining nodes. Is this CP or AP? Justify in one sentence.

**Q2.** You have a 5-node Cassandra cluster. A client writes with `consistency = QUORUM` (W=3). Two nodes are down. Does the write succeed?

**Q3.** PACELC adds an "Else" branch to CAP. What specific latency vs consistency trade-off does this model capture that CAP does not?

**Q4.** Name the three fault models in order from weakest to strongest. Which one does Raft assume?

**Q5.** FLP impossibility proves that consensus is impossible in an asynchronous model. How do practical algorithms like Raft escape this impossibility?

---

## 2. Chaos Engineering

Chaos engineering is the practice of **deliberately injecting failures** into a running system to observe how it behaves. You already have a chat app from Day 5. Today we break it.

**Why bother?** You cannot know how your system behaves under failure until you test it under failure. Surprises in production cost far more than surprises in development.

---

## Hands-on Assignment (Go)

We will add a **chaos proxy** between the chat client and server that injects latency and drops packets.

### Step 1: Install toxiproxy (optional, Mac/Linux)

```bash
brew install toxiproxy
```

Or use the pure-Go approach below (no external tools needed).

### Step 2: Build a chaos proxy in Go

Create a new project `dist-sys-day10`:

```bash
mkdir dist-sys-day10
cd dist-sys-day10
go mod init day10
```

Create `chaos_proxy.go`:

```go
package main

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

var (
	latencyMs   = 0
	dropPercent = 0
)

func proxy(src net.Conn, target string) {
	dst, err := net.Dial("tcp", target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect to target: %v\n", err)
		src.Close()
		return
	}
	defer src.Close()
	defer dst.Close()

	copy := func(from, to net.Conn) {
		buf := make([]byte, 4096)
		for {
			n, err := from.Read(buf)
			if n > 0 {
				// Drop packet?
				if rand.Intn(100) < dropPercent {
					fmt.Printf("💀 Dropped %d bytes\n", n)
					continue
				}
				// Inject latency
				if latencyMs > 0 {
					time.Sleep(time.Duration(latencyMs) * time.Millisecond)
				}
				to.Write(buf[:n])
			}
			if err == io.EOF || err != nil {
				return
			}
		}
	}

	go copy(src, dst)
	copy(dst, src)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("usage: chaos_proxy <listen-port> <target-host:port> <latency-ms> [drop-percent]")
		os.Exit(1)
	}
	listenPort := os.Args[1]
	target := os.Args[2]
	latencyMs, _ = strconv.Atoi(os.Args[3])
	if len(os.Args) > 4 {
		dropPercent, _ = strconv.Atoi(os.Args[4])
	}

	l, err := net.Listen("tcp", ":"+listenPort)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Chaos proxy on :%s → %s | latency=%dms drop=%d%%\n",
		listenPort, target, latencyMs, dropPercent)

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go proxy(conn, target)
	}
}
```

### Step 3: Run the chaos experiment

Open four terminals:

**Terminal 1** — start the Day 5 gRPC chat server on `:50051`:
```bash
cd grpc-demo && go run server.go
```

**Terminal 2** — start the chaos proxy (500ms latency, 20% drop, forwarding to server):
```bash
go run chaos_proxy.go 50052 localhost:50051 500 20
```

**Terminal 3 & 4** — connect chat clients through the chaos proxy:
```bash
go run client.go Alice --server=localhost:50052
go run client.go Bob   --server=localhost:50052
```

### Step 4: Observe and document

Send messages through the chaos proxy. Record what you see:

- Are messages delayed? By how much?
- Are any messages lost entirely (no echo on other clients)?
- Does the client crash or hang? Or does it recover gracefully?
- What happens when you raise the drop rate to 50%?

### Step 5: Add a timeout to the client

Modify `client.go` to add a gRPC call timeout:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
client.SendMessage(ctx, &pb.Message{Body: text, From: username})
```

Does this improve client resilience? What error do you see when the proxy drops the packet?

---

## Your Next Step

Excellent — you have now named the enemy (CAP, PACELC, fault models) and experienced it firsthand (chaos proxy). Next week we go deeper into **why ordering is hard** and introduce the tools that solve it: **Lamport clocks** and **vector clocks**.
