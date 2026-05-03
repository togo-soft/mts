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

func TestEngine_Query_TagFilter(t *testing.T) {
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

	// 写入不同 host 的数据
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
			Tags:        map[string]string{"host": "server2"},
			Timestamp:   now + 1e9,
			Fields:      map[string]any{"usage": 90.0},
		},
	}

	err = engine.WriteBatch(points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// 只查询 host=server1 的数据
	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   now,
		EndTime:     now + 2e9,
		Tags:        map[string]string{"host": "server1"}, // Tag 过滤
	}

	resp, err := engine.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(resp.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.Rows))
	}

	if resp.Rows[0].Tags["host"] != "server1" {
		t.Errorf("expected host=server1, got host=%s", resp.Rows[0].Tags["host"])
	}
}

func TestEngine_Query_Concurrent(t *testing.T) {
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

	// 写入多个 shard 的数据
	baseTime := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*int64(time.Hour), // 每小时一个 shard
			Fields:      map[string]any{"usage": float64(i)},
		}
		if err := engine.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 查询跨多个 shard
	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + 100*int64(time.Hour),
		Limit:       50,
	}

	resp, err := engine.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(resp.Rows) != 50 {
		t.Errorf("expected 50 rows, got %d", len(resp.Rows))
	}
}

func TestEngine_Query_Pagination(t *testing.T) {
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

	// 写入 100 条数据
	points := make([]*types.Point, 100)
	for i := 0; i < 100; i++ {
		points[i] = &types.Point{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + int64(i)*1e9,
			Fields:      map[string]any{"usage": float64(i)},
		}
	}
	err = engine.WriteBatch(points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// 查询前 10 条
	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   now,
		EndTime:     now + 100*1e9,
		Offset:      0,
		Limit:       10,
	}

	resp, err := engine.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(resp.Rows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(resp.Rows))
	}

	if resp.TotalCount != 100 {
		t.Errorf("expected TotalCount=100, got %d", resp.TotalCount)
	}

	if !resp.HasMore {
		t.Errorf("expected HasMore=true")
	}

	// 查询第 20-30 条 (offset=20, limit=10)
	req.Offset = 20
	resp, err = engine.Query(req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(resp.Rows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(resp.Rows))
	}

	// 验证是第 20-29 条数据
	if resp.Rows[0].Fields["usage"] != float64(20) {
		t.Errorf("expected first row usage=20, got %v", resp.Rows[0].Fields["usage"])
	}
}
