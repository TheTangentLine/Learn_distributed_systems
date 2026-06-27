package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	pb "grpc-chat/proto"
)

type chatServer struct {
	pb.UnimplementedChatServiceServer
	mu      sync.Mutex
	streams []pb.ChatService_SubscribeServer
}

func (s *chatServer) SendMessage(_ context.Context, msg *pb.Message) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stream := range s.streams {
		if err := stream.Send(msg); err != nil {
			fmt.Printf("send error: %v\n", err)
		}
	}
	fmt.Printf("[%s]: %s\n", msg.From, msg.Body)
	return &pb.Ack{Status: "ok"}, nil
}

func (s *chatServer) Subscribe(req *pb.SubscribeRequest, stream pb.ChatService_SubscribeServer) error {
	s.mu.Lock()
	s.streams = append(s.streams, stream)
	s.mu.Unlock()

	fmt.Printf(">> %s joined\n", req.Username)

	// Broadcast join notification to all existing streams
	s.mu.Lock()
	for _, st := range s.streams {
		st.Send(&pb.Message{From: "server", Body: req.Username + " joined"})
	}
	s.mu.Unlock()

	// Block until the client disconnects
	<-stream.Context().Done()
	fmt.Printf("<< %s left\n", req.Username)

	// Remove this stream from the list
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
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterChatServiceServer(s, &chatServer{})
	fmt.Println("Chat server listening on :50051")
	log.Fatal(s.Serve(lis))
}
