// tests/e2e/grpc_write_query/main.go
// gRPC 端到端测试：写入 10K 条数据并通过查询 API 验证数据完整性
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"codeberg.org/micro-ts/mts/internal/api"
	"codeberg.org/micro-ts/mts/internal/engine"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
	"codeberg.org/micro-ts/mts/types"
)

const (
	serverAddr = "127.0.0.1:20261"
	count      = 10000
	timeout    = 60 * time.Second
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_grpc_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// 启动 gRPC 服务端（在后台 goroutine）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go startServer(ctx, tmpDir)

	// 等待服务端启动
	time.Sleep(500 * time.Millisecond)

	// 连接服务端
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	client := types.NewMicroTSClient(conn)

	// 创建带超时的上下文
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 创建数据库和 measurement
	if _, err := client.CreateDatabase(ctx, &types.CreateDatabaseRequest{Database: "db1"}); err != nil {
		fmt.Printf("CreateDatabase failed: %v\n", err)
		os.Exit(1)
	}
	if _, err := client.CreateMeasurement(ctx, &types.CreateMeasurementRequest{
		Database:    "db1",
		Measurement: "cpu",
	}); err != nil {
		fmt.Printf("CreateMeasurement failed: %v\n", err)
		os.Exit(1)
	}

	// 生成测试数据
	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	expectedPoints := make([]*types.Point, count)

	fmt.Printf("gRPC Write/Query E2E Test - %d points\n", count)
	fmt.Printf("====================================\n\n")

	// 写入阶段
	metrics.GC()
	memBeforeWrite := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBeforeWrite))

	timer := metrics.NewWriteSummary(count)
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		expectedPoints[i] = p

		req := &types.WriteRequest{
			Database:    p.Database,
			Measurement: p.Measurement,
			Tags:        p.Tags,
			Timestamp:   p.Timestamp,
			Fields:      p.Fields,
		}

		resp, err := client.Write(ctx, req)
		if err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
		if !resp.Success {
			fmt.Printf("Write failed at %d: %s\n", i, resp.Error)
			os.Exit(1)
		}
	}
	timer.Finish()

	metrics.GC()
	memAfterWrite := metrics.ReadMemStats()
	writeDelta := metrics.CalcDelta(memBeforeWrite, memAfterWrite)

	fmt.Printf("%s\n", timer.Format())
	fmt.Printf("Write completed in %v, TPS: %.2f\n", timer.Elapsed(), timer.TPS())
	fmt.Printf("After write: %s\n", metrics.FormatMemStats(memAfterWrite))
	fmt.Printf("Write memory delta: %s\n\n", writeDelta.Format())

	// 等待数据落盘
	fmt.Printf("Waiting 2s for data flush...\n")
	time.Sleep(2 * time.Second)

	// 查询阶段
	metrics.GC()
	memBeforeRead := metrics.ReadMemStats()
	fmt.Printf("Before query: %s\n", metrics.FormatMemStats(memBeforeRead))

	queryTimer := metrics.NewTimer()
	stream, err := client.QueryRange(ctx, &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		os.Exit(1)
	}
	var queryResp []*types.Row
	for {
		row, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Query recv failed: %v\n", err)
			os.Exit(1)
		}
		queryResp = append(queryResp, row)
	}
	readElapsed := queryTimer.Elapsed()

	metrics.GC()
	memAfterRead := metrics.ReadMemStats()
	readDelta := metrics.CalcDelta(memBeforeRead, memAfterRead)

	fmt.Printf("Query completed in %v\n", readElapsed)
	fmt.Printf("After query: %s\n", metrics.FormatMemStats(memAfterRead))
	fmt.Printf("Query memory delta: %s\n\n", readDelta.Format())

	// 验证阶段
	fmt.Printf("Verifying data integrity...\n")

	if len(queryResp) != count {
		fmt.Printf("FAIL: expected %d points, got %d\n", count, len(queryResp))
		os.Exit(1)
	}

	errorCount := 0
	for i, row := range queryResp {
		expected := expectedPoints[i]

		// 验证 timestamp
		if row.Timestamp != expected.Timestamp {
			fmt.Printf("row %d: timestamp mismatch (expected %d, got %d)\n", i, expected.Timestamp, row.Timestamp)
			errorCount++
			if errorCount > 10 {
				break
			}
		}

		// 验证 fields
		for name, expectedVal := range expected.Fields {
			actualVal, ok := row.Fields[name]
			if !ok {
				fmt.Printf("row %d: missing field %s\n", i, name)
				errorCount++
				if errorCount > 10 {
					break
				}
				continue
			}
			if !fieldValueEqual(actualVal, expectedVal) {
				fmt.Printf("row %d: field %s mismatch (expected %v, got %v)\n", i, name, expectedVal.GetValue(), actualVal.GetValue())
				errorCount++
				if errorCount > 10 {
					break
				}
			}
		}
		if errorCount > 10 {
			break
		}
	}

	if errorCount > 0 {
		fmt.Printf("FAIL: %d errors found\n", errorCount)
		os.Exit(1)
	}

	fmt.Printf("\n====================================\n")
	fmt.Printf("PASS: gRPC E2E test successful!\n")
	fmt.Printf("  Written: %d points\n", count)
	fmt.Printf("  Write TPS: %.2f\n", timer.TPS())
	fmt.Printf("  Query Rows: %d\n", len(queryResp))

	// 统计存储
	dataDir := filepath.Join(tmpDir, "db1", "cpu")
	fmt.Printf("\n%s\n", metrics.FormatStorageReport(dataDir, count, 80))
}

// startServer 启动 gRPC 服务端
func startServer(ctx context.Context, dataDir string) {
	eng, err := engine.New(&engine.Config{
		DataDir:       dataDir,
		ShardDuration: time.Hour,
	})
	if err != nil {
		fmt.Printf("Failed to create engine: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = eng.Close() }()

	lis, err := net.Listen("tcp", serverAddr)
	if err != nil {
		fmt.Printf("Failed to listen: %v\n", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	types.RegisterMicroTSServer(s, api.New(eng))

	// 在后台运行服务器
	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		fmt.Printf("Server error: %v\n", err)
	}
}

// fieldValueEqual 比较两个 FieldValue 的值是否相等
func fieldValueEqual(a, b *types.FieldValue) bool {
	if a == nil || b == nil {
		return a == b
	}
	switch av := a.Value.(type) {
	case *types.FieldValue_IntValue:
		if bv, ok := b.Value.(*types.FieldValue_IntValue); ok {
			return av.IntValue == bv.IntValue
		}
	case *types.FieldValue_FloatValue:
		if bv, ok := b.Value.(*types.FieldValue_FloatValue); ok {
			return av.FloatValue == bv.FloatValue
		}
	case *types.FieldValue_StringValue:
		if bv, ok := b.Value.(*types.FieldValue_StringValue); ok {
			return av.StringValue == bv.StringValue
		}
	case *types.FieldValue_BoolValue:
		if bv, ok := b.Value.(*types.FieldValue_BoolValue); ok {
			return av.BoolValue == bv.BoolValue
		}
	}
	return false
}
