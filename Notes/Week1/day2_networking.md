**Day 2: Networking Refresher (TCP vs UDP)**

In distributed systems, 90% of your problems will be at the **Transport Layer**. You generally have two choices: **TCP** and **UDP**.

**1. The Theory**

**TCP (Transmission Control Protocol)** - _The Reliable One_

- **Guarantees:** Delivery, Order, and Error Checking. If I send packets A, B, C, they arrive as A, B, C.

- **Cost:** It requires a "Handshake" to start. It's slower because if a packet is lost, it stops and waits to re-send it (Head-of-Line blocking).

- **Use Case:** Databases, REST APIs, where data must be correct.

**UDP (User Datagram Protocol)** - _The Fast One_

- **Guarantees:** None. Fire and forget. Packets might arrive out of order or not at all.

- **Cost:** Extremely low overhead.

- **Use Case:** Video streaming, DNS, Gaming, Heartbeat signals between servers.

**The 3-Way Handshake (Why latency hits you twice):** Before sending data via TCP, your client does this:

1. **SYN**: "Can I talk to you?"

2. **SYN-ACK**: "Yes, I'm listening."

3. **ACK**: "Okay, here comes data."

_This means even sending 1 byte of data requires a full round-trip of latency before the data even moves._

---

**Day 2: Hands-on Assignment (Raw TCP)**

We are abandoning `net/http` today. We will use Go's `net` package to build a raw **Concurrent TCP Echo Server**.

**Key Distributed Concept: Concurrency**. We need to handle multiple clients at the same time without blocking the main thread.

**Step 1: Create** `tcp_server.go`

```Go

package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func handleConnection(c net.Conn) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())

	// Close the connection when this function exits
	defer c.Close()

	for {
		// Read data until a newline
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		temp := strings.TrimSpace(string(netData))
		if temp == "STOP" {
			break
		}

		// Write data back to the connection (The "Echo")
		fmt.Fprintf(c, "Server Echo: %s\n", temp)
	}
}

func main() {
	PORT := ":8080"
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()
	fmt.Println("TCP Server is listening on " + PORT)

	for {
		// Block until a new client connects
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		// Go routine! This allows us to handle this client
		// while instantly going back to loop for the next client.
		go handleConnection(c)
	}
}
```

**Step 2: Test it without writing a client** You don't always need to write a Go client to test TCP. You can use standard network tools.

1. Run the server: `go run tcp_server.go`
2. Open a new terminal and use `telnet` or `nc` (netcat):

```bash
nc localhost 8080
```

_(Type anything and hit enter. The server should reply)._

**Step 3: The Challenge** Open **three** separate terminal windows and connect them all to your server using `nc localhost 8080`.

1. Type in Window 1.
2. Type in Window 2.
3. Type in Window 3.

**Question:** Does Window 1 block Window 2? (Look at the `go handleConnection(c)` line in the code. If we removed the keyword `go`, what would happen to the second client?)

**Your Next Step**

Once you confirm that the `go` keyword is the magic sauce for concurrency here, we are ready for **Day 3**.

We will look at **RPC (Remote Procedure Calls)** and **gRPC**. We will stop sending raw text strings and start sending structured data, just like real microservices do.

**Answer:** In Go, these are called **Goroutines**. They are much lighter than OS threads (you can spawn thousands of them), which is why Go is so good for distributed systems.
