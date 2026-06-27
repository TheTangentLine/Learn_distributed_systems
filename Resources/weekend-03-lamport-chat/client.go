package main

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "lamport-chat/proto"
)

// ─── Min-heap ordered by LamportTs ───────────────────────────────────────────

type MessageHeap []*pb.Message

func (h MessageHeap) Len() int            { return len(h) }
func (h MessageHeap) Less(i, j int) bool  { return h[i].LamportTs < h[j].LamportTs }
func (h MessageHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *MessageHeap) Push(x interface{}) { *h = append(*h, x.(*pb.Message)) }
func (h *MessageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// OrderedDisplay buffers incoming messages and flushes them in Lamport order.
type OrderedDisplay struct {
	mu  sync.Mutex
	buf *MessageHeap
}

func NewOrderedDisplay() *OrderedDisplay {
	h := &MessageHeap{}
	heap.Init(h)
	return &OrderedDisplay{buf: h}
}

func (od *OrderedDisplay) Add(msg *pb.Message) {
	od.mu.Lock()
	defer od.mu.Unlock()
	heap.Push(od.buf, msg)
}

// FlushLoop prints buffered messages in Lamport order every 50ms.
// A small window allows late-arriving messages to be sorted before display.
func (od *OrderedDisplay) FlushLoop() {
	for {
		time.Sleep(50 * time.Millisecond)
		od.mu.Lock()
		for od.buf.Len() > 0 {
			msg := heap.Pop(od.buf).(*pb.Message)
			fmt.Printf("\r[LC=%d][%s]: %s\n> ", msg.LamportTs, msg.From, msg.Body)
		}
		od.mu.Unlock()
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run client.go <username>")
	}
	username := os.Args[1]

	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Username: username})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	display := NewOrderedDisplay()
	go display.FlushLoop()

	// Receive messages and add them to the ordered buffer
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				fmt.Printf("\nreceive error: %v\n", err)
				return
			}
			display.Add(msg)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("Connected as %s (Lamport ordering enabled). Type a message.\n> ", username)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "" {
			fmt.Print("> ")
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := client.SendMessage(ctx, &pb.Message{Body: text, From: username})
		cancel()
		if err != nil {
			fmt.Printf("send error: %v\n", err)
		}
		fmt.Print("> ")
	}
}
