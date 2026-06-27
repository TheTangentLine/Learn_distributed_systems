package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	pb "lamport-chat/proto"
)

// LamportClock is a thread-safe Lamport logical clock.
type LamportClock struct {
	mu    sync.Mutex
	value uint64
}

func (lc *LamportClock) Receive(msgTS uint64) uint64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if msgTS > lc.value {
		lc.value = msgTS
	}
	lc.value++
	return lc.value
}

type chatServer struct {
	pb.UnimplementedChatServiceServer
	mu      sync.Mutex
	streams []pb.ChatService_SubscribeServer
	clock   *LamportClock
}

func (s *chatServer) SendMessage(_ context.Context, msg *pb.Message) (*pb.Ack, error) {
	// Advance the server clock: take the max of client's ts and server's, then +1
	ts := s.clock.Receive(msg.LamportTs)
	msg.LamportTs = ts

	fmt.Printf("[LC=%d][%s]: %s\n", ts, msg.From, msg.Body)

	s.mu.Lock()
	streams := append([]pb.ChatService_SubscribeServer{}, s.streams...)
	s.mu.Unlock()

	// Broadcast to all subscribers. For the second subscriber onward, add a
	// random delay to simulate out-of-order network delivery so the client
	// min-heap ordering becomes observable.
	for i, stream := range streams {
		st := stream
		idx := i
		go func() {
			if idx > 0 {
				time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
			}
			if err := st.Send(msg); err != nil {
				fmt.Printf("send error: %v\n", err)
			}
		}()
	}

	return &pb.Ack{Status: "ok"}, nil
}

func (s *chatServer) Subscribe(req *pb.SubscribeRequest, stream pb.ChatService_SubscribeServer) error {
	s.mu.Lock()
	s.streams = append(s.streams, stream)
	s.mu.Unlock()

	fmt.Printf(">> %s joined\n", req.Username)
	<-stream.Context().Done()
	fmt.Printf("<< %s left\n", req.Username)

	s.mu.Lock()
	for i, st := range s.streams {
		if st == stream {
			s.streams = append(s.streams[:i], s.streams[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterChatServiceServer(s, &chatServer{clock: &LamportClock{}})
	fmt.Println("Lamport chat server on :50051")
	log.Fatal(s.Serve(lis))
}
