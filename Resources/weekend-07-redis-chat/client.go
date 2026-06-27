package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "redis-chat/proto"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run client.go <username> [server-addr]")
	}
	username := os.Args[1]
	serverAddr := "localhost:50051"
	if len(os.Args) >= 3 {
		serverAddr = os.Args[2]
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect to %s: %v", serverAddr, err)
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Username: username})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	// Receive messages from the Redis-backed broadcast
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("\nServer closed.")
				return
			}
			if err != nil {
				fmt.Printf("\nReceive error: %v\n", err)
				return
			}
			fmt.Printf("\r[%s]: %s\n> ", msg.From, msg.Body)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("Connected as %s to %s. Type a message.\n> ", username, serverAddr)
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
