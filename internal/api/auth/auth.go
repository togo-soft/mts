// Package auth 提供 gRPC 认证拦截器。
package auth

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// AuthorizationHeader gRPC metadata 中的授权头名称
	AuthorizationHeader = "authorization"
	// BearerPrefix Bearer Token 前缀
	BearerPrefix = "Bearer "
)

// APIKeyAuthenticator API Key 认证器。
type APIKeyAuthenticator struct {
	key []byte
}

// NewAPIKeyAuthenticator 创建 API Key 认证器。
func NewAPIKeyAuthenticator(key string) *APIKeyAuthenticator {
	return &APIKeyAuthenticator{key: []byte(key)}
}

// isValid 检查 key 是否有效。
func (a *APIKeyAuthenticator) isValid(key string) bool {
	if len(a.key) == 0 {
		return true // 未配置密钥时跳过验证
	}
	return subtle.ConstantTimeCompare([]byte(key), a.key) == 1
}

// extractBearerToken 从 metadata 中提取 Bearer token。
func extractBearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	authValues := md.Get(AuthorizationHeader)
	if len(authValues) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := authValues[0]
	if !strings.HasPrefix(authHeader, BearerPrefix) {
		return "", status.Error(codes.Unauthenticated, "invalid authorization format, expected 'Bearer <token>'")
	}

	return strings.TrimPrefix(authHeader, BearerPrefix), nil
}

// UnaryServerInterceptor 返回 unary 拦截器函数。
func (a *APIKeyAuthenticator) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Health Check 跳过认证（K8s 探针需要）
		if info.FullMethod == "/grpc.health.v1.Health/Check" ||
			info.FullMethod == "/grpc.health.v1.Health/Watch" {
			return handler(ctx, req)
		}

		token, err := extractBearerToken(ctx)
		if err != nil {
			return nil, err
		}

		if !a.isValid(token) {
			return nil, status.Error(codes.Unauthenticated, "invalid API key")
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor 返回 stream 拦截器函数。
func (a *APIKeyAuthenticator) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Health Watch 跳过认证
		if info.FullMethod == "/grpc.health.v1.Health/Watch" {
			return handler(srv, ss)
		}

		token, err := extractBearerToken(ss.Context())
		if err != nil {
			return err
		}

		if !a.isValid(token) {
			return status.Error(codes.Unauthenticated, "invalid API key")
		}

		return handler(srv, ss)
	}
}
