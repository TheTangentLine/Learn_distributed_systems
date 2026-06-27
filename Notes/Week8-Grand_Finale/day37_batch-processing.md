# Day 37: Batch Processing & The Google File System

## 1. The GFS Design Philosophy

The Google File System (Ghemawat, Gobioff, Leung 2003) was built on top of the MapReduce observation: at Google's scale, node failures are **the norm**, not the exception. A 1000-node cluster expects several failures per day.

Key design decisions:

| Decision | Rationale |
|----------|-----------|
| Large chunk size (64 MB) | Reduces metadata overhead; matches workloads (large batch files, not millions of tiny files) |
| Single master for metadata | Simplifies consistency; metadata fits in RAM |
| Append-only writes | Avoids random write contention; aligns with log-structured processing |
| 3× replication by default | Tolerates 2 simultaneous replica failures |
| Checksums per chunk | Detects silent bit-rot |

### The single-master bottleneck

The master holds all file metadata in memory. It is not a write bottleneck (clients read chunks directly from chunkservers after one master lookup) but it is a potential SPOF. GFS mitigated this with a shadow master and write-ahead log, but this is why modern systems (HDFS NameNode HA, Colossus) invested in master replication.

## 2. Chunk-Based Storage

```
File: /data/logs/2024-01-01.log (200 MB)
  Chunk 0: bytes 0–63 MB  → chunkserver A (primary), B, C (replicas)
  Chunk 1: bytes 64–127 MB → chunkserver D (primary), A, E (replicas)
  Chunk 2: bytes 128–191 MB → chunkserver B (primary), C, D (replicas)
  Chunk 3: bytes 192–200 MB → chunkserver A (primary), B, E (replicas)
```

A client that wants to read byte 100,000,000:
1. Asks master: "which chunkserver holds chunk 1 of `/data/logs/2024-01-01.log`?"
2. Contacts chunkserver D directly to read the bytes.
3. Master is not in the read path after the initial lookup.

---

## Hands-on Assignment (Go)

We implement a simplified chunked file writer that mimics the GFS chunk structure.

### Step 1: Set up the project

```bash
mkdir dist-sys-day37
cd dist-sys-day37
go mod init day37
```

### Step 2: Create `chunked_store.go`

```go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const chunkSize = 64 // 64 bytes per chunk (in real GFS: 64 MB)

type ChunkIndex struct {
	Filename string        `json:"filename"`
	Chunks   []ChunkEntry  `json:"chunks"`
}

type ChunkEntry struct {
	ID     int    `json:"id"`
	File   string `json:"file"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

func writeChunks(filename, content string) (*ChunkIndex, error) {
	idx := &ChunkIndex{Filename: filename}
	offset := 0
	chunkID := 0

	for offset < len(content) {
		end := offset + chunkSize
		if end > len(content) {
			end = len(content)
		}
		chunk := content[offset:end]
		chunkFile := fmt.Sprintf("chunk_%d.bin", chunkID)

		if err := os.WriteFile(chunkFile, []byte(chunk), 0644); err != nil {
			return nil, err
		}

		idx.Chunks = append(idx.Chunks, ChunkEntry{
			ID:     chunkID,
			File:   chunkFile,
			Offset: offset,
			Length: len(chunk),
		})

		fmt.Printf("  Written chunk %d: bytes %d–%d → %s\n",
			chunkID, offset, end-1, chunkFile)
		offset = end
		chunkID++
	}

	// Write index file
	indexData, _ := json.MarshalIndent(idx, "", "  ")
	if err := os.WriteFile(filename+".index.json", indexData, 0644); err != nil {
		return nil, err
	}
	fmt.Printf("  Index written: %s.index.json\n", filename)
	return idx, nil
}

func readFromIndex(indexFile string) (string, error) {
	data, err := os.ReadFile(indexFile)
	if err != nil {
		return "", err
	}

	var idx ChunkIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return "", err
	}

	var sb strings.Builder
	for _, entry := range idx.Chunks {
		chunkData, err := os.ReadFile(entry.File)
		if err != nil {
			return "", fmt.Errorf("missing chunk %d: %v", entry.ID, err)
		}
		sb.Write(chunkData)
	}
	return sb.String(), nil
}

func main() {
	content := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 6)
	fmt.Printf("Writing %d bytes in %d-byte chunks\n\n", len(content), chunkSize)

	idx, err := writeChunks("testfile.log", content)
	if err != nil {
		fmt.Println("Error writing:", err)
		return
	}
	fmt.Printf("\nTotal chunks: %d\n\n", len(idx.Chunks))

	// Read back and verify
	recovered, err := readFromIndex("testfile.log.index.json")
	if err != nil {
		fmt.Println("Error reading:", err)
		return
	}

	if recovered == content {
		fmt.Println("✅ Read-back verified: content matches original")
	} else {
		fmt.Println("❌ Content mismatch!")
	}

	// Simulate a missing chunk
	os.Remove("chunk_1.bin")
	fmt.Println("\n=== Simulating missing chunk_1.bin ===")
	_, err = readFromIndex("testfile.log.index.json")
	if err != nil {
		fmt.Printf("Read error (as expected): %v\n", err)
		fmt.Println("In a real GFS, the reader would request chunk_1 from a replica.")
	}

	// Cleanup
	for _, entry := range idx.Chunks {
		os.Remove(entry.File)
	}
	os.Remove("testfile.log.index.json")
}
```

### Step 3: Run it

```bash
go run chunked_store.go
```

---

## Review

1. In GFS, why does the master not participate in the data read path after the initial lookup? What would happen to read throughput if the master did participate in every read?

2. GFS uses append-only writes. Name two advantages of this constraint from a concurrency perspective.
