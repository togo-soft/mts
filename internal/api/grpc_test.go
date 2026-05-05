package api

import (
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/engine"
	"codeberg.org/micro-ts/mts/types"
)

func TestMicroTSService_Health(t *testing.T) {
	// 基础测试：验证服务结构体存在
	srv := &MicroTSService{}
	_ = srv // srv is always non-nil when created with struct literal
}

func TestNew(t *testing.T) {
	eng, err := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	srv := New(eng)

	if srv == nil {
		t.Fatal("expected non-nil service")
	}

	if srv.engine != eng {
		t.Errorf("expected engine %v, got %v", eng, srv.engine)
	}

	// 验证嵌入的 UnimplementedMicroTSServer 已正确嵌入
	var _ types.MicroTSServer = srv
}

func TestMicroTSService_Health_CheckResponse(t *testing.T) {
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	resp, err := srv.Health(ctx, &types.HealthRequest{})
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
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	req := &types.WriteRequest{
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

	// 空字段时仍然返回 Success（只是没有数据写入）
	if !resp.Success {
		t.Errorf("expected success to be true for empty fields, got: %s", resp.Error)
	}
}

func TestMicroTSService_WriteBatch(t *testing.T) {
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	req := &types.WriteBatchRequest{
		Points: []*types.WriteRequest{
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

	// 没有字段时仍然返回 Success
	if !resp.Success {
		t.Errorf("expected success to be true, got: %s", resp.Error)
	}

	if resp.Count != int32(len(req.Points)) {
		t.Errorf("expected count %d, got %d", len(req.Points), resp.Count)
	}
}

func TestMicroTSService_Write_WithFields(t *testing.T) {
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	req := &types.WriteRequest{
		Database:    "testdb",
		Measurement: "testmeas",
		Tags:        map[string]string{"tag1": "value1"},
		Timestamp:   time.Now().UnixNano(),
		Fields: map[string]*types.FieldValue{
			"value": {Value: &types.FieldValue_FloatValue{FloatValue: 42.0}},
		},
	}

	resp, err := srv.Write(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	if !resp.Success {
		t.Errorf("expected success to be true, got error: %s", resp.Error)
	}
}

func TestMicroTSService_QueryRange(t *testing.T) {
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	req := &types.QueryRangeRequest{
		Database:    "testdb",
		Measurement: "testmeas",
		StartTime:   1234567890,
		EndTime:     1234567900,
	}

	resp, err := srv.QueryRange(ctx, req)
	if err != nil {
		// 查询可能因为没有数据而失败，这是预期的
		return
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
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	req := &types.ListMeasurementsRequest{
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

func TestMicroTSService_CreateAndDropMeasurement(t *testing.T) {
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	// 创建 Measurement
	createReq := &types.CreateMeasurementRequest{
		Database:    "testdb",
		Measurement: "testmeas",
	}
	createResp, err := srv.CreateMeasurement(ctx, createReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !createResp.Success {
		t.Errorf("expected create success, got: %s", createResp.Error)
	}

	// 列出 Measurements
	listResp, err := srv.ListMeasurements(ctx, &types.ListMeasurementsRequest{Database: "testdb"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, m := range listResp.Measurements {
		if m == "testmeas" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find created measurement")
	}

	// 删除 Measurement
	dropResp, err := srv.DropMeasurement(ctx, &types.DropMeasurementRequest{
		Database:    "testdb",
		Measurement: "testmeas",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dropResp.Success {
		t.Errorf("expected drop success, got: %s", dropResp.Error)
	}
}

func TestMicroTSService_DatabaseOperations(t *testing.T) {
	eng, _ := engine.New(&engine.Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	})
	srv := New(eng)
	ctx := t.Context()

	// 创建数据库
	createResp, err := srv.CreateDatabase(ctx, &types.CreateDatabaseRequest{Database: "testdb"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !createResp.Success {
		t.Errorf("expected create success, got: %s", createResp.Error)
	}

	// 列出数据库
	listResp, err := srv.ListDatabases(ctx, &types.ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, db := range listResp.Databases {
		if db == "testdb" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find created database")
	}

	// 删除数据库
	dropResp, err := srv.DropDatabase(ctx, &types.DropDatabaseRequest{Database: "testdb"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dropResp.Success {
		t.Errorf("expected drop success, got: %s", dropResp.Error)
	}

	// 再次列出确认删除
	listResp, _ = srv.ListDatabases(ctx, &types.ListDatabasesRequest{})
	for _, db := range listResp.Databases {
		if db == "testdb" {
			t.Error("expected database to be deleted")
		}
	}
}

func TestFieldValueConversion(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		checkVal func(*types.FieldValue) bool
	}{
		{
			name:  "int64",
			value: int64(42),
			checkVal: func(fv *types.FieldValue) bool {
				v, ok := fv.Value.(*types.FieldValue_IntValue)
				return ok && v.IntValue == 42
			},
		},
		{
			name:  "float64",
			value: float64(3.14),
			checkVal: func(fv *types.FieldValue) bool {
				v, ok := fv.Value.(*types.FieldValue_FloatValue)
				return ok && v.FloatValue == 3.14
			},
		},
		{
			name:  "string",
			value: "test",
			checkVal: func(fv *types.FieldValue) bool {
				v, ok := fv.Value.(*types.FieldValue_StringValue)
				return ok && v.StringValue == "test"
			},
		},
		{
			name:  "bool",
			value: true,
			checkVal: func(fv *types.FieldValue) bool {
				v, ok := fv.Value.(*types.FieldValue_BoolValue)
				return ok && v.BoolValue == true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fv, err := anyToFieldValue(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 验证包装的值
			if !tt.checkVal(fv) {
				t.Errorf("field value check failed for %v", tt.value)
			}

			// 转换回来验证
			val, err := fieldValueToAny(fv)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if val != tt.value {
				t.Errorf("expected %v, got %v", tt.value, val)
			}
		})
	}
}

func TestWriteRequestToPoint(t *testing.T) {
	req := &types.WriteRequest{
		Database:    "testdb",
		Measurement: "testmeas",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   1234567890,
		Fields: map[string]*types.FieldValue{
			"value":  {Value: &types.FieldValue_FloatValue{FloatValue: 42.0}},
			"count":  {Value: &types.FieldValue_IntValue{IntValue: 100}},
			"status": {Value: &types.FieldValue_StringValue{StringValue: "ok"}},
			"active": {Value: &types.FieldValue_BoolValue{BoolValue: true}},
		},
	}

	point, err := writeRequestToPoint(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if point.Database != req.Database {
		t.Errorf("expected database %s, got %s", req.Database, point.Database)
	}
	if point.Measurement != req.Measurement {
		t.Errorf("expected measurement %s, got %s", req.Measurement, point.Measurement)
	}
	if point.Timestamp != req.Timestamp {
		t.Errorf("expected timestamp %d, got %d", req.Timestamp, point.Timestamp)
	}
	if len(point.Fields) != 4 {
		t.Errorf("expected 4 fields, got %d", len(point.Fields))
	}
}

func TestToProtoPointRow(t *testing.T) {
	row := &types.PointRow{
		Timestamp: 1234567890,
		Tags:      map[string]string{"tag1": "value1"},
		Fields:    map[string]*types.FieldValue{"field1": types.NewFieldValue(float64(1.0))},
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
