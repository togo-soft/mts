// cmd/server/main.go
package main

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"micro-ts/internal/api"
	pb "micro-ts/internal/api/pb"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMicroTSServer(s, api.NewMicroTSService(nil))

	fmt.Println("micro-ts server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
