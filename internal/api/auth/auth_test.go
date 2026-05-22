// Package auth 提供 gRPC 认证拦截器测试。
package auth

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAPIKeyAuthenticator_IsValid(t *testing.T) {
	// 测试无密钥配置
	authNoKey := NewAPIKeyAuthenticator("")
	if !authNoKey.isValid("any-key") {
		t.Error("should pass validation when no key is configured")
	}

	// 测试有效密钥
	authWithKey := NewAPIKeyAuthenticator("secret-key")
	if !authWithKey.isValid("secret-key") {
		t.Error("should pass validation with correct key")
	}

	// 测试无效密钥
	if authWithKey.isValid("wrong-key") {
		t.Error("should fail validation with incorrect key")
	}
}

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name      string
		metadata  metadata.MD
		wantToken string
		wantErr   codes.Code
	}{
		{
			name:      "valid bearer token",
			metadata:  metadata.New(map[string]string{"authorization": "Bearer secret-key"}),
			wantToken: "secret-key",
			wantErr:   0,
		},
		{
			name:      "missing header",
			metadata:  metadata.New(map[string]string{}),
			wantToken: "",
			wantErr:   codes.Unauthenticated,
		},
		{
			name:      "invalid format",
			metadata:  metadata.New(map[string]string{"authorization": "Basic secret-key"}),
			wantToken: "",
			wantErr:   codes.Unauthenticated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), tt.metadata)
			token, err := extractBearerToken(ctx)

			if tt.wantErr != 0 {
				if st, ok := status.FromError(err); !ok || st.Code() != tt.wantErr {
					t.Errorf("extractBearerToken() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			if token != tt.wantToken {
				t.Errorf("extractBearerToken() = %v, want %v", token, tt.wantToken)
			}
		})
	}
}

func TestUnaryServerInterceptor_HealthCheck(t *testing.T) {
	auth := NewAPIKeyAuthenticator("secret-key")
	interceptor := auth.UnaryServerInterceptor()

	// 创建 mock server info，指向 Health Check 方法
	info := &grpc.UnaryServerInfo{
		FullMethod: "/grpc.health.v1.Health/Check",
	}

	var called bool
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}

	// 不带任何 metadata 调用（Health Check 应该跳过认证）
	ctx := context.Background()
	_, err := interceptor(ctx, nil, info, handler)
	if err != nil {
		t.Errorf("Health Check should not fail: %v", err)
	}
	if !called {
		t.Error("handler should be called for Health Check")
	}
}

func TestUnaryServerInterceptor_RequiresAuth(t *testing.T) {
	auth := NewAPIKeyAuthenticator("secret-key")
	interceptor := auth.UnaryServerInterceptor()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/mts.v1.MTS/Write",
	}

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}

	// 无 metadata 应该失败
	ctx := context.Background()
	_, err := interceptor(ctx, nil, info, handler)
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("should return Unauthenticated error, got: %v", err)
	}

	// 错误密钥应该失败
	ctx = metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		"authorization": "Bearer wrong-key",
	}))
	_, err = interceptor(ctx, nil, info, handler)
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("should return Unauthenticated error for wrong key, got: %v", err)
	}

	// 正确密钥应该成功
	ctx = metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		"authorization": "Bearer secret-key",
	}))
	_, err = interceptor(ctx, nil, info, handler)
	if err != nil {
		t.Errorf("should pass with correct key: %v", err)
	}
}
