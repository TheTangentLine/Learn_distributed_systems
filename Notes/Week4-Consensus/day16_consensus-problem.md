# Day 16: The Consensus Problem

## 1. What Is Consensus?

**Consensus** is the problem of getting a group of nodes to agree on a single value — even when some nodes may crash or messages may be lost.

### Formal properties

A correct consensus algorithm must satisfy all three:

- **Termination:** every non-faulty node eventually decides on a value.
- **Validity:** the decided value was proposed by some node (no values appear from nowhere).
- **Agreement:** no two non-faulty nodes decide on different values.

### Where consensus is required

| Use case | What is agreed upon |
|----------|---------------------|
| Leader election | which node is the current leader |
| Distributed lock | who holds the lock |
| Configuration change | new cluster membership |
| Distributed transaction | commit or abort |
| Linearizable key-value store | the current value of a key |

## 2. Why It Is Hard — FLP Impossibility

Fischer, Lynch, Paterson (1985): in a **purely asynchronous** system (unbounded message delays, no timing assumptions), **consensus is impossible** if even one process may crash.

**Intuition:** In an asynchronous system, you cannot tell whether a node is dead or just very slow. If you wait forever for it to respond, you violate Termination. If you proceed without it, you may violate Agreement (what if it is alive and decides differently?).

**How real systems escape FLP:** they assume **partial synchrony** — messages are usually delivered within some timeout, even if that bound is not guaranteed. Raft uses election timeouts. If a follower hears nothing for 150–300ms, it assumes the leader crashed and starts a new election. This assumption is usually correct but not provably correct in all network conditions — which is why consensus algorithms can briefly lose liveness (stop making progress) during extreme network turbulence.

---

## Hands-on Assignment (Go)

We will demonstrate FLP in miniature: three goroutines try to agree on a value using only message passing. Without a timeout mechanism, they can block forever.

### Step 1: Set up the project

```bash
mkdir dist-sys-day16
cd dist-sys-day16
go mod init day16
```

### Step 2: Create `consensus.go` — the blocking version

```go
package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Proposal struct {
	from  int
	value string
}

func node(id int, proposal string, in <-chan Proposal, out []chan<- Proposal, decided chan<- string) {
	// Send our proposal to all peers
	for i, ch := range out {
		if i != id {
			ch <- Proposal{from: id, value: proposal}
		}
	}

	// Wait to hear from all other nodes (blocks forever if one crashes!)
	received := map[int]string{id: proposal}
	for len(received) < 3 {
		msg := <-in
		received[msg.from] = msg.value
		fmt.Printf("Node %d received from Node %d: %q\n", id, msg.from, msg.value)
	}

	// Simple majority: pick the most common value
	counts := map[string]int{}
	for _, v := range received {
		counts[v]++
	}
	best := ""
	for v, c := range counts {
		if c > counts[best] {
			best = v
		}
	}
	fmt.Printf("Node %d decided: %q\n", id, best)
	decided <- best
}

func main() {
	channels := []chan Proposal{
		make(chan Proposal, 10),
		make(chan Proposal, 10),
		make(chan Proposal, 10),
	}

	outs := [][]chan<- Proposal{
		{channels[1], channels[2]},
		{channels[0], channels[2]},
		{channels[0], channels[1]},
	}

	decided := make(chan string, 3)
	proposals := []string{"A", "A", "B"}

	// Simulate Node 2 crashing before it sends (comment this out to see normal operation)
	crashNode := 2
	fmt.Printf("Simulating crash of Node %d before it sends\n", crashNode)

	for i := 0; i < 3; i++ {
		if i == crashNode {
			continue // Node 2 never runs — it crashed
		}
		go node(i, proposals[i], channels[i], outs[i], decided)
	}

	// With crashNode == 2: Node 0 and Node 1 will block forever waiting for Node 2
	// Add a timeout to see this clearly
	select {
	case v := <-decided:
		fmt.Println("First decision:", v)
	case <-time.After(2 * time.Second):
		fmt.Println("TIMEOUT: nodes are blocked waiting for crashed node")
		fmt.Println("This is FLP in action. A timeout-based approach (like Raft) is needed.")
	}

	_ = rand.Int() // suppress import warning
}
```

### Step 3: Run it

```bash
go run consensus.go
```

You will see the timeout fire because Node 0 and Node 1 are waiting forever for Node 2.

### Step 4: Fix it with a timeout

In the `node` function, replace `msg := <-in` with:

```go
select {
case msg := <-in:
    received[msg.from] = msg.value
case <-time.After(500 * time.Millisecond):
    fmt.Printf("Node %d timed out waiting for a peer\n", id)
    break
}
```

Now nodes can make progress with the values they have. This is the essential insight behind Raft's election timeout.

---

## Review

1. What is the difference between **liveness** and **safety** in a distributed system? Which property does FLP say you must give up?

2. Raft uses a randomized election timeout of 150–300ms. Why randomized? What happens if all followers have the same exact timeout value?
