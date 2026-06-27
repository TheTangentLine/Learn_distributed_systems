# Day 30: Distributed Transactions & Sagas

## 1. ACID in a Distributed World

On a single database, ACID transactions are straightforward:

- **Atomicity:** either all writes commit or none do.
- **Consistency:** constraints are enforced at commit time.
- **Isolation:** concurrent transactions don't see each other's partial state.
- **Durability:** committed data survives crashes.

Across multiple services or databases, **atomicity** is the problem. 2PC (Day 17) provides distributed atomicity but blocks on coordinator failure. Modern microservice architectures often avoid distributed transactions entirely by using a looser model: **Sagas**.

## 2. The Saga Pattern

A Saga breaks a long-running distributed transaction into a sequence of local transactions, each with a corresponding **compensating transaction** (a rollback action).

**Choreography:** each service publishes events after its local transaction. The next service listens for that event and starts its transaction.

**Orchestration:** a central coordinator sends commands to each service and coordinates their responses.

### Example: hotel booking

```
Step 1: charge credit card   → compensating: issue refund
Step 2: reserve flight       → compensating: cancel flight
Step 3: reserve hotel        → compensating: cancel hotel

If step 3 fails:
  cancel hotel (step 3 comp — no-op)
  cancel flight (step 2 comp)
  issue refund (step 1 comp)
```

The key insight: **there is no global rollback**. Each service either succeeds or runs its own compensating action. The system eventually reaches a consistent state — but it may pass through intermediate inconsistent states.

---

## Hands-on Assignment (Go)

We implement a 2-step saga with a simple orchestrator.

### Step 1: Set up the project

```bash
mkdir dist-sys-day30
cd dist-sys-day30
go mod init day30
```

### Step 2: Create the three service handlers

```go
package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type ChargeStore struct {
	mu      sync.Mutex
	charges map[string]string
}

var charges = &ChargeStore{charges: make(map[string]string)}
var reservations = make(map[string]string)
var resMu sync.Mutex

// POST /charge?orderID=X&amount=Y
func handleCharge(w http.ResponseWriter, r *http.Request) {
	orderID := r.URL.Query().Get("orderID")
	amount := r.URL.Query().Get("amount")
	charges.mu.Lock()
	charges.charges[orderID] = amount
	charges.mu.Unlock()
	fmt.Printf("[charge] charged %s for order %s\n", amount, orderID)
	fmt.Fprintf(w, "charged")
}

// POST /refund?orderID=X (compensating transaction for charge)
func handleRefund(w http.ResponseWriter, r *http.Request) {
	orderID := r.URL.Query().Get("orderID")
	charges.mu.Lock()
	delete(charges.charges, orderID)
	charges.mu.Unlock()
	fmt.Printf("[charge] refunded order %s\n", orderID)
	fmt.Fprintf(w, "refunded")
}

// POST /reserve?orderID=X&item=Y  (fails randomly 30% of the time to test saga)
func handleReserve(w http.ResponseWriter, r *http.Request) {
	orderID := r.URL.Query().Get("orderID")
	item := r.URL.Query().Get("item")
	if rand.Float32() < 0.3 {
		http.Error(w, "reservation failed (simulated)", 500)
		fmt.Printf("[reserve] FAILED for order %s\n", orderID)
		return
	}
	resMu.Lock()
	reservations[orderID] = item
	resMu.Unlock()
	fmt.Printf("[reserve] reserved %s for order %s\n", item, orderID)
	fmt.Fprintf(w, "reserved")
}

// POST /cancel?orderID=X
func handleCancel(w http.ResponseWriter, r *http.Request) {
	orderID := r.URL.Query().Get("orderID")
	resMu.Lock()
	delete(reservations, orderID)
	resMu.Unlock()
	fmt.Printf("[reserve] cancelled order %s\n", orderID)
	fmt.Fprintf(w, "cancelled")
}
```

### Step 3: Create the Saga orchestrator

```go
func runSaga(orderID, amount, item string) error {
	base := "http://localhost:8080"

	// Step 1: charge
	resp, err := http.Get(fmt.Sprintf("%s/charge?orderID=%s&amount=%s", base, orderID, amount))
	if err != nil || resp.StatusCode != 200 {
		return fmt.Errorf("charge failed for %s", orderID)
	}
	fmt.Printf("[saga] step 1 (charge) succeeded for %s\n", orderID)

	// Step 2: reserve
	resp, err = http.Get(fmt.Sprintf("%s/reserve?orderID=%s&item=%s", base, orderID, item))
	if err != nil || resp.StatusCode != 200 {
		// Compensate: issue refund
		fmt.Printf("[saga] step 2 (reserve) failed — running compensations\n")
		http.Get(fmt.Sprintf("%s/refund?orderID=%s", base, orderID))
		return fmt.Errorf("reservation failed for %s, refund issued", orderID)
	}
	fmt.Printf("[saga] step 2 (reserve) succeeded for %s\n", orderID)
	fmt.Printf("[saga] ✅ order %s complete\n", orderID)
	return nil
}

func main() {
	http.HandleFunc("/charge", handleCharge)
	http.HandleFunc("/refund", handleRefund)
	http.HandleFunc("/reserve", handleReserve)
	http.HandleFunc("/cancel", handleCancel)

	go func() {
		fmt.Println("Services listening on :8080")
		http.ListenAndServe(":8080", nil)
	}()

	time.Sleep(100 * time.Millisecond)

	// Run 5 saga attempts
	for i := 1; i <= 5; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		err := runSaga(orderID, "100USD", "flight-NYC")
		if err != nil {
			fmt.Printf("[saga] ❌ %v\n", err)
		}
		fmt.Println()
		time.Sleep(50 * time.Millisecond)
	}
}
```

### Step 4: Run it

```bash
go run main.go
```

About 30% of orders will fail at Step 2 (reservation). Verify that in every failure case, a refund is issued — the charge is always compensated.

---

## Your Next Step

You have now mastered data storage and replication. Next week we move into **architecture patterns**: how do we wire all these nodes together using messaging and patterns that handle the chaos gracefully?
