// internal/engine/engine_test.go
package engine

import (
	"testing"
	"time"

	"micro-ts/internal/types"
)

func TestEngine_Open(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	if engine == nil {
		t.Errorf("expected non-nil engine")
	}
}

func TestEngine_Close(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, _ := NewEngine(cfg)
	err := engine.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestEngine_Write(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]any{"usage": 85.5},
	}

	err = engine.Write(point)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestEngine_Query(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	now := time.Now().UnixNano()

	// 写入测试数据
	points := []*types.Point{
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now,
			Fields:      map[string]any{"usage": 85.5},
		},
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + 1e9,
			Fields:      map[string]any{"usage": 90.0},
		},
	}

	err = engine.WriteBatch(points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// 查询
	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   now,
		EndTime:     now + 2e9,
	}

	resp, err := engine.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(resp.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(resp.Rows))
	}
}

func TestEngine_WriteBatch(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	points := []*types.Point{
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   time.Now().UnixNano(),
			Fields:      map[string]any{"usage": 85.5},
		},
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server2"},
			Timestamp:   time.Now().UnixNano() + 1e9,
			Fields:      map[string]any{"usage": 90.0},
		},
	}

	err = engine.WriteBatch(points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
}

func TestEngine_Query_FieldProjection(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	now := time.Now().UnixNano()

	// 写入带有多个字段的数据
	points := []*types.Point{
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now,
			Fields:      map[string]any{"usage": 85.5, "count": int64(100), "temperature": 65.0},
		},
	}

	err = engine.WriteBatch(points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// 只查询 usage 和 count 字段
	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   now,
		EndTime:     now + 1e9,
		Fields:      []string{"usage", "count"}, // 字段过滤
	}

	resp, err := engine.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(resp.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.Rows))
	}

	// 验证只有指定字段
	row := resp.Rows[0]
	if _, ok := row.Fields["usage"]; !ok {
		t.Errorf("expected usage field")
	}
	if _, ok := row.Fields["count"]; !ok {
		t.Errorf("expected count field")
	}
	if _, ok := row.Fields["temperature"]; ok {
		t.Errorf("temperature field should not be present")
	}
}
