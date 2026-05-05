// cmd/server/main.go
package main

import (
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"

	"micro-ts/internal/api"
	pb "micro-ts/internal/api/pb"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	s := grpc.NewServer()
	pb.RegisterMicroTSServer(s, api.New(nil))

	logger.Info("micro-ts server listening", slog.String("addr", ":50051"))
	if err := s.Serve(lis); err != nil {
		logger.Error("failed to serve", slog.Any("error", err))
		os.Exit(1)
	}
}
