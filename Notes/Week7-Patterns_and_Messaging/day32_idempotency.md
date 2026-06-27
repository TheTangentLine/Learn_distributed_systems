# Day 32: Idempotency

## 1. The Double-Send Problem

In a distributed system, every request can fail in three ways:

1. The request was **never received** by the server.
2. The server **processed it but the response was lost** on the way back.
3. The server **failed mid-processing** and the client can't tell if it committed.

In all three cases, the client's only safe option is to **retry**. But a naive retry can cause duplicate processing: you charge a customer twice, reserve a seat twice, send an email twice.

**Idempotency** means: processing the same request multiple times has the same effect as processing it once.

## 2. Idempotency Keys

The pattern:
1. The client generates a unique `Idempotency-Key` (UUID) for each logical operation.
2. The client includes this key in every attempt (original + retries).
3. The server stores the key and its result. If it sees the same key again, it returns the stored result without reprocessing.

This gives the client **at-least-once delivery** semantics from the network layer + **idempotent processing** at the server = effectively exactly-once behavior.

## 3. HTTP Idempotency by Method

Some HTTP methods are idempotent by definition:
- `GET`: reading data does not change state.
- `PUT`: replacing a resource with a given value — doing it twice gives the same state.
- `DELETE`: deleting something twice leaves it deleted.

`POST` is **not** idempotent by default — every call creates a new resource. You must add an `Idempotency-Key` header to make a POST idempotent.

---

## Hands-on Assignment (Go)

### Step 1: Set up the project

```bash
mkdir dist-sys-day32
cd dist-sys-day32
go mod init day32
```

### Step 2: Create `main.go`

```go
package main

import (
	"fmt"
	"net/http"
	"sync"
)

type chargeRecord struct {
	amount string
	status string
}

type ChargeService struct {
	mu      sync.Mutex
	charges map[string]*chargeRecord // idempotency-key → result
	total   int
}

func (s *ChargeService) Charge(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	amount := r.URL.Query().Get("amount")

	if key == "" {
		http.Error(w, "Idempotency-Key header required", 400)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we have already processed this key
	if existing, ok := s.charges[key]; ok {
		fmt.Fprintf(w, "[DUPLICATE] already processed: amount=%s status=%s\n",
			existing.amount, existing.status)
		return
	}

	// Process the charge
	s.total++
	s.charges[key] = &chargeRecord{amount: amount, status: "success"}
	fmt.Fprintf(w, "[NEW] charged %s (total charges in DB: %d)\n", amount, s.total)
	fmt.Printf("Server: processed charge key=%s amount=%s\n", key, amount)
}

func (s *ChargeService) Status(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(w, "Total unique charges processed: %d\n", s.total)
	fmt.Fprintf(w, "Idempotency keys stored: %d\n", len(s.charges))
}

func main() {
	svc := &ChargeService{
		charges: make(map[string]*chargeRecord),
	}
	http.HandleFunc("/charge", svc.Charge)
	http.HandleFunc("/status", svc.Status)
	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}
```

### Step 3: Run and test

```bash
go run main.go
```

**Test 1 — new charge:**
```bash
curl -H "Idempotency-Key: pay-uuid-001" "localhost:8080/charge?amount=100"
# → [NEW] charged 100 (total charges in DB: 1)
```

**Test 2 — retry with same key (simulates network retry):**
```bash
curl -H "Idempotency-Key: pay-uuid-001" "localhost:8080/charge?amount=100"
# → [DUPLICATE] already processed: amount=100 status=success
```

**Test 3 — fire same request 5 times:**
```bash
for i in {1..5}; do
  curl -H "Idempotency-Key: pay-uuid-001" "localhost:8080/charge?amount=100"
done
curl "localhost:8080/status"
# Total unique charges processed: 1  ← only 1 charge, despite 5 requests
```

**Test 4 — new charge with different key:**
```bash
curl -H "Idempotency-Key: pay-uuid-002" "localhost:8080/charge?amount=200"
curl "localhost:8080/status"
# Total unique charges processed: 2
```

### Step 4: Production considerations

In a real system:

- The idempotency key map is stored in Redis or a database, not in-process memory. Otherwise a server restart wipes it.
- Keys expire after a reasonable window (e.g., 24 hours) to bound storage growth.
- The key is tied to the user ID to prevent one user from using another's key.

---

## Review

1. A payment service processes 1 million transactions per day. Idempotency keys are kept for 24 hours. How many keys are in the store at peak? What storage does this require (assume 36-byte UUIDs)?

2. Is `GET /balance` idempotent? Is it safe to retry on network failure? Explain the difference between idempotent and safe.
