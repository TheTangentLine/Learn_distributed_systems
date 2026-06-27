# Day 11: Physical Clocks (Why `time.Now()` Lies)

## 1. How Computer Clocks Work

Every computer has a quartz oscillator that vibrates at approximately 32,768 Hz. A counter tracks those vibrations and converts them to a time value. The problem: quartz oscillators **drift** — a typical server clock drifts up to 200 µs per second, which compounds to about 17 seconds per day.

**NTP (Network Time Protocol)** corrects this drift by periodically comparing your clock to a stratum-0 source (GPS, atomic clock). But NTP corrections are not instantaneous and introduce their own problems:

- **Synchronization error:** even a well-configured NTP client has ±1–50ms uncertainty on the internet; ±100µs on a local LAN.
- **Clock steps:** NTP may step the clock backward to correct a large drift. Any code using `time.Now()` as a counter may see timestamps go backward.
- **Leap seconds:** the IETF occasionally inserts or deletes a second from UTC. Systems handle this by freezing or smearing the clock — either way, `time.Now()` behaves strangely for up to a day.
- **VM clock skew:** virtualized hosts steal CPU cycles from the guest. The guest clock can fall behind by hundreds of milliseconds and then jump forward.

## 2. Why This Matters for Ordering

```
Node A clock: 10:00:00.001   Node B clock: 10:00:00.000
```

If Node A's event at `.001` caused something on Node B, and Node B timestamped its response at `.000`, which event appears to have happened "first" according to physical clocks? The effect appears to precede the cause. This is impossible in physics but entirely possible in a distributed system with clock skew.

**Safe uses for physical time:**
- Human-readable timestamps ("order placed at 3:45 PM")
- TTL/expiry (tolerates ±minutes of error)
- Log correlation (approximate, not authoritative)

**Unsafe uses for physical time:**
- Determining which of two concurrent writes wins (Last Write Wins with physical clocks silently loses data)
- Ordering distributed events causally
- Snapshot consistency

---

## Hands-on Assignment (Go)

### Step 1: Create `main.go`

We run two goroutines — each acting as a separate "node" — and compare their rate of progression.

```go
package main

import (
	"fmt"
	"time"
)

func node(id int, done <-chan struct{}) {
	var count int
	prev := time.Now()
	for {
		select {
		case <-done:
			return
		case <-time.After(100 * time.Millisecond):
			now := time.Now()
			delta := now.Sub(prev)
			fmt.Printf("Node %d | count=%d | wall=%v | delta=%v\n",
				id, count, now.Format("15:04:05.000"), delta)
			prev = now
			count++
		}
	}
}

func main() {
	done := make(chan struct{})
	go node(1, done)
	go node(2, done)

	time.Sleep(2 * time.Second)
	close(done)
}
```

### Step 2: Run and observe

```bash
go run main.go
```

Both nodes tick every ~100ms but the `delta` values will differ slightly — this is the in-process equivalent of clock drift.

### Step 3: Observe a backward step (simulated)

Add this to your `node` function to simulate what a clock step backward looks like:

```go
// After count==5, simulate a backward step
if count == 5 {
    prev = prev.Add(200 * time.Millisecond) // pretend clock jumped back
    fmt.Printf("Node %d | ⚠️  Clock stepped backward!\n", id)
}
```

Watch what happens to `delta` on the next tick — it becomes negative. Any code that uses `time.Now()` as a sequence number would generate a lower number after a higher one.

### Step 4: Monotonic clock vs wall clock

Go's `time.Now()` actually contains **two** readings:

```go
t := time.Now()
fmt.Println(t)               // wall clock (can go backward)
fmt.Println(t.UnixNano())    // nanoseconds since epoch (wall clock)

// To measure elapsed time safely, use Sub — it uses the monotonic reading
start := time.Now()
time.Sleep(100 * time.Millisecond)
elapsed := time.Since(start) // uses monotonic — never negative
fmt.Println(elapsed)
```

The monotonic clock only works within a single process. Across processes or nodes, you need logical clocks.

---

## Your Next Step

Now you know _why_ physical clocks cannot be trusted for ordering. Tomorrow we learn the simplest logical clock that solves the ordering problem without any physical time at all: **Lamport Clocks**.
