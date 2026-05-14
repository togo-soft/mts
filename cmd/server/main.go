// cmd/server/main.go
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"codeberg.org/micro-ts/mts/internal/api"
	"codeberg.org/micro-ts/mts/internal/api/auth"
	"codeberg.org/micro-ts/mts/internal/engine"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	dataDir := flag.String("data-dir", "", "数据目录路径（默认从 MICROTS_DATA_DIR 环境变量读取，回退到 /var/lib/microts）")
	tlsCert := flag.String("tls-cert", "", "TLS 证书文件路径（可选，启用 TLS 需要同时指定 -tls-key）")
	tlsKey := flag.String("tls-key", "", "TLS 私钥文件路径（可选，启用 TLS 需要同时指定 -tls-cert）")
	authKey := flag.String("auth-key", "", "API 密钥（可选，未设置则不验证）")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dir := *dataDir
	if dir == "" {
		dir = os.Getenv("MICROTS_DATA_DIR")
	}
	if dir == "" {
		dir = "/var/lib/microts"
	}

	// 初始化存储引擎
	eng, err := engine.New(&engine.Config{
		DataDir:       dir,
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

	// 配置 gRPC 选项
	grpcOpts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(100),
		grpc.MaxRecvMsgSize(4 * 1024 * 1024), // 4MB
		grpc.MaxSendMsgSize(4 * 1024 * 1024), // 4MB
	}

	// 认证配置（可选）
	var authInterceptor *auth.APIKeyAuthenticator
	if *authKey != "" {
		authInterceptor = auth.NewAPIKeyAuthenticator(*authKey)
		grpcOpts = append(grpcOpts,
			grpc.UnaryInterceptor(authInterceptor.UnaryServerInterceptor()),
			grpc.StreamInterceptor(authInterceptor.StreamServerInterceptor()),
		)
		logger.Info("API key authentication enabled")
	}

	// TLS 配置（可选）
	if *tlsCert != "" && *tlsKey != "" {
		creds, err := loadTLSCredentials(*tlsCert, *tlsKey)
		if err != nil {
			logger.Error("failed to load TLS credentials", slog.Any("error", err))
			os.Exit(1)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
		logger.Info("TLS enabled")
	} else if *tlsCert != "" || *tlsKey != "" {
		logger.Error("both -tls-cert and -tls-key must be specified for TLS")
		os.Exit(1)
	}

	s := grpc.NewServer(grpcOpts...)
	types.RegisterMicroTSServer(s, api.New(eng))

	// 注册健康检查服务（用于 K8s 探针）
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, healthServer)

	// 等待信号以优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("received shutdown signal")

		// 设置服务状态为 NOT_SERVING，阻止新请求
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		// 停止 gRPC，不再接受新请求
		stopped := make(chan struct{})
		go func() {
			s.GracefulStop()
			close(stopped)
		}()

		// 超时回退：如果优雅关闭超时，强制关闭
		select {
		case <-stopped:
			logger.Info("grpc server stopped gracefully")
		case <-time.After(10 * time.Second):
			logger.Warn("grpc graceful stop timeout, forcing stop")
			s.Stop()
		}

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

// loadTLSCredentials 加载 TLS 证书和私钥。
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}

	// 创建证书池以验证客户端证书（可选）
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(certFile)
	if err == nil {
		certPool.AppendCertsFromPEM(ca)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		ClientCAs:    certPool,
		ClientAuth:   tls.NoClientCert,
	}

	return credentials.NewTLS(tlsConfig), nil
}
