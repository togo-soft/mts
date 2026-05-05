package api

import (
	"context"
	"testing"

	"micro-ts/internal/api/pb"
	"micro-ts/types"
)

func TestMicroTSService_Health(t *testing.T) {
	// 基础测试：验证服务结构体存在
	srv := &MicroTSService{}
	_ = srv // srv is always non-nil when created with struct literal
}

func TestNewMicroTSService(t *testing.T) {
	engine := "test-engine"
	srv := NewMicroTSService(engine)

	if srv == nil {
		t.Fatal("expected non-nil service")
	}

	if srv.engine != engine {
		t.Errorf("expected engine %v, got %v", engine, srv.engine)
	}

	// 验证嵌入的 UnimplementedMicroTSServer 已正确嵌入
	var _ pb.MicroTSServer = srv
}

func TestMicroTSService_Health_CheckResponse(t *testing.T) {
	srv := NewMicroTSService(nil)
	ctx := context.Background()

	resp, err := srv.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if !resp.Healthy {
		t.Error("expected healthy to be true")
	}

	if resp.Version == "" {
		t.Error("expected version to be set")
	}
}

func TestMicroTSService_Write(t *testing.T) {
	srv := NewMicroTSService(nil)
	ctx := context.Background()

	req := &pb.WriteRequest{
		Database:    "testdb",
		Measurement: "testmeas",
		Tags:        map[string]string{"tag1": "value1"},
		Timestamp:   1234567890,
		Fields:      nil,
	}

	resp, err := srv.Write(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if !resp.Success {
		t.Error("expected success to be true")
	}
}

func TestMicroTSService_WriteBatch(t *testing.T) {
	srv := NewMicroTSService(nil)
	ctx := context.Background()

	req := &pb.WriteBatchRequest{
		Points: []*pb.WriteRequest{
			{
				Database:    "testdb",
				Measurement: "testmeas",
				Tags:        map[string]string{"tag1": "value1"},
				Timestamp:   1234567890,
			},
			{
				Database:    "testdb",
				Measurement: "testmeas",
				Tags:        map[string]string{"tag2": "value2"},
				Timestamp:   1234567891,
			},
		},
	}

	resp, err := srv.WriteBatch(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if !resp.Success {
		t.Error("expected success to be true")
	}

	if resp.Count != int32(len(req.Points)) {
		t.Errorf("expected count %d, got %d", len(req.Points), resp.Count)
	}
}

func TestMicroTSService_QueryRange(t *testing.T) {
	srv := NewMicroTSService(nil)
	ctx := context.Background()

	req := &pb.QueryRangeRequest{
		Database:    "testdb",
		Measurement: "testmeas",
		StartTime:   1234567890,
		EndTime:     1234567900,
	}

	resp, err := srv.QueryRange(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if resp.Database != req.Database {
		t.Errorf("expected database %s, got %s", req.Database, resp.Database)
	}

	if resp.Measurement != req.Measurement {
		t.Errorf("expected measurement %s, got %s", req.Measurement, resp.Measurement)
	}

	if resp.StartTime != req.StartTime {
		t.Errorf("expected startTime %d, got %d", req.StartTime, resp.StartTime)
	}

	if resp.EndTime != req.EndTime {
		t.Errorf("expected endTime %d, got %d", req.EndTime, resp.EndTime)
	}
}

func TestMicroTSService_ListMeasurements(t *testing.T) {
	srv := NewMicroTSService(nil)
	ctx := context.Background()

	req := &pb.ListMeasurementsRequest{
		Database: "testdb",
	}

	resp, err := srv.ListMeasurements(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if resp.Measurements == nil {
		t.Error("expected non-nil measurements slice")
	}
}

func TestToProtoPointRow(t *testing.T) {
	row := &types.PointRow{
		Timestamp: 1234567890,
		Tags:      map[string]string{"tag1": "value1"},
		Fields:    map[string]any{"field1": float64(1.0)},
	}

	protoRow := ToProtoPointRow(row)

	if protoRow == nil {
		t.Fatal("expected non-nil proto row")
	}

	if protoRow.Timestamp != row.Timestamp {
		t.Errorf("expected timestamp %d, got %d", row.Timestamp, protoRow.Timestamp)
	}

	if protoRow.Tags["tag1"] != "value1" {
		t.Errorf("expected tag1=value1, got %s", protoRow.Tags["tag1"])
	}
}

func TestToProtoPointRow_Nil(t *testing.T) {
	protoRow := ToProtoPointRow(nil)

	if protoRow != nil {
		t.Error("expected nil for nil input")
	}
}
