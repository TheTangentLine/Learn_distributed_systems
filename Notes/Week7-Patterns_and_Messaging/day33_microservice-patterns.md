# Day 33: Microservice Patterns

## 1. Service Discovery

When you have 50 microservices running across 200 containers, how does Service A find Service B's current IP and port?

### Hardcoded (bad)

```go
http.Get("http://192.168.1.45:8080/pay")
```

IPs change whenever containers restart.

### Environment variables (better)

```go
payURL := os.Getenv("PAYMENT_SERVICE_URL")
```

Works for simple deployments. Still requires manual updates.

### DNS-based discovery (standard)

In Kubernetes, every service gets a stable DNS name:

```
http://payment-service.default.svc.cluster.local/pay
```

The cluster DNS resolves to the current ClusterIP, which load-balances across healthy pods.

## 2. Circuit Breaker

A circuit breaker prevents a slow or failing downstream service from cascading failure to your service. It has three states:

- **Closed:** requests pass through normally. Failure count is tracked.
- **Open:** after `failureThreshold` failures in a window, the circuit opens. All requests fail immediately (fast fail) without hitting the downstream.
- **Half-Open:** after a timeout, one request is let through. If it succeeds, the circuit closes. If it fails, it opens again.

```
Client → CircuitBreaker → DownstreamService
            ↑ closed: pass through
            ↑ open:   fail fast, return error immediately
            ↑ half-open: trial request
```

## 3. Bulkhead

Named after ship bulkheads (watertight compartments), this pattern isolates resources so one failing consumer cannot exhaust the entire pool.

**Example:** instead of one HTTP client for all downstream calls, use separate connection pools (or goroutine pools) per service. If the payment service is slow and fills up its pool of 20 goroutines, the user-profile service still has its own 20 goroutines and continues working.

---

## Hands-on Assignment (Go)

We implement a `CircuitBreaker` struct.

### Step 1: Set up the project

```bash
mkdir dist-sys-day33
cd dist-sys-day33
go mod init day33
```

### Step 2: Create `circuit_breaker.go`

```go
package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type State int

const (
	StateClosed   State = iota
	StateOpen     State = iota
	StateHalfOpen State = iota
)

func (s State) String() string {
	return [...]string{"Closed", "Open", "HalfOpen"}[s]
}

var ErrCircuitOpen = errors.New("circuit breaker is open")

type CircuitBreaker struct {
	mu               sync.Mutex
	state            State
	failures         int
	failureThreshold int
	timeout          time.Duration
	lastFailure      time.Time
}

func NewCircuitBreaker(failureThreshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: failureThreshold,
		timeout:          timeout,
	}
}

func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()

	switch cb.state {
	case StateOpen:
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = StateHalfOpen
			fmt.Println("  [CB] → HalfOpen (trying one request)")
		} else {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
	case StateHalfOpen:
		// Only one trial request allowed — pass through
	}

	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		if cb.state == StateHalfOpen || cb.failures >= cb.failureThreshold {
			cb.state = StateOpen
			fmt.Printf("  [CB] → Open (failures=%d)\n", cb.failures)
		}
		return err
	}

	// Success
	cb.failures = 0
	cb.state = StateClosed
	return nil
}

func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}
```

### Step 3: Create `main.go`

```go
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

func main() {
	cb := NewCircuitBreaker(3, 500*time.Millisecond)

	// Downstream function that fails 60% of the time
	downstream := func() error {
		if rand.Float32() < 0.6 {
			return errors.New("downstream timeout")
		}
		return nil
	}

	fmt.Println("=== Sending 20 requests ===")
	for i := 1; i <= 20; i++ {
		err := cb.Call(downstream)
		state := cb.State()
		if errors.Is(err, ErrCircuitOpen) {
			fmt.Printf("Request %2d: FAST FAIL (circuit %s)\n", i, state)
		} else if err != nil {
			fmt.Printf("Request %2d: ERROR: %v (circuit %s)\n", i, err, state)
		} else {
			fmt.Printf("Request %2d: success (circuit %s)\n", i, state)
		}
		time.Sleep(50 * time.Millisecond)
	}

	fmt.Println("\n=== Waiting for circuit to recover (600ms) ===")
	time.Sleep(600 * time.Millisecond)

	// Force a success to close the circuit
	successDownstream := func() error { return nil }
	err := cb.Call(successDownstream)
	fmt.Printf("Trial request: err=%v, state=%s\n", err, cb.State())
}
```

### Step 4: Run it

```bash
go run circuit_breaker.go main.go
```

Observe: after 3 failures the circuit opens and subsequent requests get `FAST FAIL` without calling the downstream. After 600ms, one trial request goes through. If it succeeds, the circuit closes.

---

## Review

1. What is the advantage of fast-failing with an open circuit vs just having a short timeout on every request?

2. A service has two downstream dependencies: a database (critical) and a recommendation engine (nice to have). Should they share a circuit breaker or have separate ones? What is the bulkhead argument for separate ones?
