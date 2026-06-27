# Day 4: Serialization (JSON vs Protobuf)

## 1. The Problem: Data on the Wire

When two processes communicate, they must agree on how to convert in-memory data structures into bytes for the wire, and back again. This is **serialization** (encoding) and **deserialization** (decoding).

You already have two options on the table: JSON (from Day 1) and Protobuf (from Day 3). Today we measure the difference.

## 2. JSON — Human-readable, Field-name heavy

JSON sends the full field name with every value:

```json
{"id": 1, "name": "Kha", "email": "kha@example.com", "active": true}
```

- Human-readable and easy to debug.
- Every field name is repeated on every message — expensive at scale.
- Parsing is CPU-intensive: you scan for `"`, `:`, `,`, `{`, `}`.

## 3. Protobuf — Binary, Field-tag efficient

Protobuf sends only field tags (integers) and values:

```
08 01          // field 1 (id), varint, value 1
12 03 4B 68 61 // field 2 (name), len=3, "Kha"
1A 0F 6B 68 61 40 ... // field 3 (email), len=15
20 01          // field 4 (active), varint, value 1 (true)
```

- Not human-readable, but 2–10× smaller than JSON for the same data.
- Parsing is a simple binary scan — much faster than JSON.
- **Forward compatibility:** adding a new field with a new tag number is safe. Old decoders skip unknown tags.
- **Backward compatibility:** removing a field is safe as long as you never reuse its tag number.

---

## Hands-on Assignment (Go)

### Step 1: Set up the project

```bash
mkdir dist-sys-day4
cd dist-sys-day4
go mod init day4
```

Add the Protobuf dependency:

```bash
go get google.golang.org/protobuf
```

### Step 2: Create the Protobuf schema

Create `user.proto`:

```protobuf
syntax = "proto3";
package day4;
option go_package = "./";

message User {
  int32  id     = 1;
  string name   = 2;
  string email  = 3;
  bool   active = 4;
}
```

Generate the Go code:

```bash
protoc --go_out=. --go_opt=paths=source_relative user.proto
```

### Step 3: Create `main.go` — benchmark both formats

```go
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

type UserJSON struct {
	ID     int32  `json:"id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Active bool   `json:"active"`
}

func main() {
	const iterations = 100_000

	userJSON := &UserJSON{ID: 1, Name: "Kha Truong", Email: "kha@example.com", Active: true}
	userProto := &User{Id: 1, Name: "Kha Truong", Email: "kha@example.com", Active: true}

	// --- JSON benchmark ---
	start := time.Now()
	var jsonBytes []byte
	for i := 0; i < iterations; i++ {
		jsonBytes, _ = json.Marshal(userJSON)
	}
	jsonDuration := time.Since(start)

	// --- Protobuf benchmark ---
	start = time.Now()
	var protoBytes []byte
	for i := 0; i < iterations; i++ {
		protoBytes, _ = proto.Marshal(userProto)
	}
	protoDuration := time.Since(start)

	fmt.Printf("JSON   size: %d bytes,  time for %d iterations: %v\n", len(jsonBytes), iterations, jsonDuration)
	fmt.Printf("Proto  size: %d bytes,  time for %d iterations: %v\n", len(protoBytes), iterations, protoDuration)
	fmt.Printf("Size ratio: %.1fx smaller with Protobuf\n", float64(len(jsonBytes))/float64(len(protoBytes)))
}
```

### Step 4: Run it

```bash
go run .
```

You should see output similar to:

```
JSON   size: 63 bytes,  time for 100000 iterations: 45ms
Proto  size: 30 bytes,  time for 100000 iterations: 12ms
Size ratio: 2.1x smaller with Protobuf
```

### Step 5: Forward-compatibility experiment

Add a new field to `user.proto` without changing existing field tags:

```protobuf
message User {
  int32  id        = 1;
  string name      = 2;
  string email     = 3;
  bool   active    = 4;
  string role      = 5;  // NEW field
}
```

Regenerate the code. Now serialize a `User` with `role = "admin"` set. Then **decode the bytes using the old struct** (comment out the `role` field and regenerate into a separate file). The old decoder should silently skip field tag `5` and return a valid struct without error.

_This is what "forward compatibility" means: new writers, old readers._

---

## Review

1. Your service sends 1 million user objects per minute. At 63 bytes each (JSON), that is 63 MB/min of wire traffic. At 30 bytes each (Protobuf), it is 30 MB/min. At what scale does this difference start to matter in real money (cloud egress costs ~$0.09/GB)?

2. Why is it dangerous to reuse a Protobuf field tag number after a field has been removed?
