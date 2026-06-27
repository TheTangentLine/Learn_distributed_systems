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

// proxy forwards bytes between src and target, injecting latency and drops.
func proxy(src net.Conn, target string) {
	dst, err := net.Dial("tcp", target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot connect to target %s: %v\n", target, err)
		src.Close()
		return
	}
	defer src.Close()
	defer dst.Close()

	copyWithChaos := func(from, to net.Conn, direction string) {
		buf := make([]byte, 4096)
		for {
			n, err := from.Read(buf)
			if n > 0 {
				if rand.Intn(100) < dropPercent {
					fmt.Printf("[%s] dropped %d bytes\n", direction, n)
					// Drop the packet — don't forward
				} else {
					if latencyMs > 0 {
						time.Sleep(time.Duration(latencyMs) * time.Millisecond)
					}
					if _, werr := to.Write(buf[:n]); werr != nil {
						return
					}
				}
			}
			if err == io.EOF || err != nil {
				return
			}
		}
	}

	done := make(chan struct{}, 2)
	go func() { copyWithChaos(src, dst, "client→server"); done <- struct{}{} }()
	go func() { copyWithChaos(dst, src, "server→client"); done <- struct{}{} }()
	<-done // when either side closes, tear down both
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: chaos_proxy <listen-port> <target-host:port> <latency-ms> [drop-percent]")
		os.Exit(1)
	}

	listenPort := os.Args[1]
	target := os.Args[2]

	var err error
	latencyMs, err = strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Fprintln(os.Stderr, "latency-ms must be an integer")
		os.Exit(1)
	}
	if len(os.Args) >= 5 {
		dropPercent, _ = strconv.Atoi(os.Args[4])
		if dropPercent < 0 || dropPercent > 100 {
			fmt.Fprintln(os.Stderr, "drop-percent must be 0–100")
			os.Exit(1)
		}
	}

	l, err := net.Listen("tcp", ":"+listenPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Chaos proxy :%s → %s | latency=%dms drop=%d%%\n",
		listenPort, target, latencyMs, dropPercent)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "accept error: %v\n", err)
			continue
		}
		go proxy(conn, target)
	}
}
