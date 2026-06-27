package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	pb "redis-chat/proto"
)

const redisChannel = "chat:room:1"

// wireMessage is the JSON payload published to Redis.
type wireMessage struct {
	From string `json:"from"`
	Body string `json:"body"`
}

type chatServer struct {
	pb.UnimplementedChatServiceServer
	mu      sync.Mutex
	streams []pb.ChatService_SubscribeServer
	rdb     *redis.Client
	ctx     context.Context
}

// SendMessage publishes the message to Redis instead of broadcasting directly.
// This decouples the receive path from the broadcast path.
func (s *chatServer) SendMessage(_ context.Context, msg *pb.Message) (*pb.Ack, error) {
	payload, err := json.Marshal(wireMessage{From: msg.From, Body: msg.Body})
	if err != nil {
		return nil, err
	}
	if err := s.rdb.Publish(s.ctx, redisChannel, payload).Err(); err != nil {
		return nil, fmt.Errorf("redis publish: %w", err)
	}
	fmt.Printf("[redis→publish] [%s]: %s\n", msg.From, msg.Body)
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

// startBroadcaster subscribes to the Redis channel and fans messages out
// to all connected gRPC streams. Runs in its own goroutine.
func (s *chatServer) startBroadcaster() {
	sub := s.rdb.Subscribe(s.ctx, redisChannel)
	ch := sub.Channel()
	fmt.Printf("[broadcaster] subscribed to Redis channel %q\n", redisChannel)

	for redisMsg := range ch {
		var wm wireMessage
		if err := json.Unmarshal([]byte(redisMsg.Payload), &wm); err != nil {
			fmt.Printf("[broadcaster] malformed message: %v\n", err)
			continue
		}

		s.mu.Lock()
		streams := append([]pb.ChatService_SubscribeServer{}, s.streams...)
		s.mu.Unlock()

		for _, stream := range streams {
			if err := stream.Send(&pb.Message{From: wm.From, Body: wm.Body}); err != nil {
				fmt.Printf("[broadcaster] send error: %v\n", err)
			}
		}
	}
}

func main() {
	port := flag.String("port", "50051", "gRPC listen port")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	ctx := context.Background()

	// Verify Redis connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot connect to Redis at %s: %v\nStart Redis with: docker run -d -p 6379:6379 redis:7", *redisAddr, err)
	}
	fmt.Printf("Connected to Redis at %s\n", *redisAddr)

	srv := &chatServer{rdb: rdb, ctx: ctx}

	// Start the Redis-to-gRPC broadcast bridge
	go srv.startBroadcaster()

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterChatServiceServer(s, srv)
	fmt.Printf("Redis-backed chat server on :%s\n", *port)
	log.Fatal(s.Serve(lis))
}
