// cmd/server/main.go
package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"codeberg.org/micro-ts/mts/internal/api"
	"codeberg.org/micro-ts/mts/internal/engine"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// 初始化存储引擎
	eng, err := engine.New(&engine.Config{
		DataDir:       "/var/lib/microts",
		ShardDuration: 7 * 24 * time.Hour,
	})
	if err != nil {
		logger.Error("failed to create engine", slog.Any("error", err))
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", ":2026")
	if err != nil {
		logger.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	s := grpc.NewServer()
	types.RegisterMicroTSServer(s, api.New(eng))

	// 等待信号以优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("received shutdown signal")

		// 先停止 gRPC，不再接受新请求
		s.GracefulStop()

		// 刷盘所有内存数据
		if err := eng.Flush(); err != nil {
			logger.Error("flush failed during shutdown", slog.Any("error", err))
		}

		// 关闭引擎，释放资源
		if err := eng.Close(); err != nil {
			logger.Error("close failed during shutdown", slog.Any("error", err))
		}

		logger.Info("shutdown complete")
	}()

	logger.Info("mts grpc server listening", slog.String("addr", ":2026"))
	if err := s.Serve(lis); err != nil {
		logger.Error("failed to serve", slog.Any("error", err))
		os.Exit(1)
	}
}
