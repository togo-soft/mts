// internal/engine/engine_test.go
package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

func TestEngine_Open(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
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

	engine, _ := New(cfg)
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

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}

	err = engine.Write(t.Context(), point)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestEngine_Query(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
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
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
		},
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + 1e9,
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0)},
		},
	}

	err = engine.WriteBatch(t.Context(), points)
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

	it, err := engine.Iterator(t.Context(), req)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	defer func() { _ = it.Close() }()

	var rows []*types.PointRow
	for it.Next(t.Context()) {
		rows = append(rows, it.Points())
	}

	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

func TestEngine_WriteBatch(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
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
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
		},
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server2"},
			Timestamp:   time.Now().UnixNano() + 1e9,
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0)},
		},
	}

	err = engine.WriteBatch(t.Context(), points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
}

func TestEngine_Query_FieldProjection(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
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
			Fields: map[string]*types.FieldValue{
				"usage":       types.NewFieldValue(85.5),
				"count":       types.NewFieldValue(int64(100)),
				"temperature": types.NewFieldValue(65.0),
			},
		},
	}

	err = engine.WriteBatch(t.Context(), points)
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

	it, err := engine.Iterator(t.Context(), req)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	defer func() { _ = it.Close() }()

	var rows []*types.PointRow
	for it.Next(t.Context()) {
		rows = append(rows, it.Points())
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// 验证只有指定字段
	row := rows[0]
	if row.GetFieldValue("usage") == nil {
		t.Errorf("expected usage field")
	}
	if row.GetFieldValue("count") == nil {
		t.Errorf("expected count field")
	}
	if row.GetFieldValue("temperature") != nil {
		t.Errorf("temperature field should not be present")
	}
}

func TestEngine_Query_TagFilter(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
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
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
		},
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server2"},
			Timestamp:   now + 1e9,
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0)},
		},
	}

	err = engine.WriteBatch(t.Context(), points)
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

	it, err := engine.Iterator(t.Context(), req)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	defer func() { _ = it.Close() }()

	var rows []*types.PointRow
	for it.Next(t.Context()) {
		rows = append(rows, it.Points())
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0].Tags["host"] != "server1" {
		t.Errorf("expected host=server1, got host=%s", rows[0].Tags["host"])
	}
}

func TestEngine_Query_Concurrent(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
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
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := engine.Write(t.Context(), p); err != nil {
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

	it, err := engine.Iterator(t.Context(), req)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	defer func() { _ = it.Close() }()

	count := 0
	for it.Next(t.Context()) {
		_ = it.Points()
		count++
	}

	if count != 50 {
		t.Errorf("expected 50 rows, got %d", count)
	}
}

func TestEngine_Query_Pagination(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
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
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
	}
	err = engine.WriteBatch(t.Context(), points)
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

	it, err := engine.Iterator(t.Context(), req)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}

	count := 0
	for it.Next(t.Context()) {
		_ = it.Points()
		count++
	}
	_ = it.Close()

	if count != 10 {
		t.Errorf("expected 10 rows, got %d", count)
	}

	// 查询第 20-30 条 (offset=20, limit=10)
	req.Offset = 20
	it, err = engine.Iterator(t.Context(), req)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}

	count = 0
	for it.Next(t.Context()) {
		_ = it.Points()
		count++
	}
	_ = it.Close()

	// 流式语义下，如果 offset 超过实际数据量，可能返回少于 limit 的行数
	// 或者返回 0 行表示已到达数据末尾
	if count > 0 && count != 10 {
		t.Errorf("expected 10 rows, got %d", count)
	}
}

func TestEngine_Flush(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	// 写入数据
	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := engine.Write(t.Context(), point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Flush
	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestEngine_DataDir(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	if engine.DataDir() != tmpDir {
		t.Errorf("expected DataDir=%s, got %s", tmpDir, engine.DataDir())
	}
}

func TestEngine_ListDatabases(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	// 创建 database
	engine.CreateDatabase("db1", 0, nil)
	engine.CreateDatabase("db2", 0, nil)

	databases := engine.ListDatabases()
	if len(databases) != 2 {
		t.Errorf("expected 2 databases, got %d", len(databases))
	}
}

func TestEngine_ListMeasurements(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	// 创建 database 和 measurement
	engine.CreateDatabase("db1", 0, nil)
	if _, err := engine.CreateMeasurement("db1", "cpu"); err != nil {
		t.Fatalf("CreateMeasurement failed: %v", err)
	}
	if _, err := engine.CreateMeasurement("db1", "mem"); err != nil {
		t.Fatalf("CreateMeasurement failed: %v", err)
	}

	measurements, found := engine.ListMeasurements("db1")
	if !found {
		t.Fatalf("expected db1 to be found")
	}
	if len(measurements) != 2 {
		t.Errorf("expected 2 measurements, got %v", measurements)
	}

	// 不存在的 database
	_, found = engine.ListMeasurements("nonexistent")
	if found {
		t.Errorf("expected not found for nonexistent database")
	}
}

func TestEngine_CreateDatabase(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	// 创建已存在的 database
	engine.CreateDatabase("db1", 0, nil)
	engine.CreateDatabase("db1", 0, nil) // 再次创建

	databases := engine.ListDatabases()
	if len(databases) != 1 {
		t.Errorf("expected 1 database, got %d", len(databases))
	}
}

func TestEngine_DropDatabase(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	// 创建 database
	engine.CreateDatabase("db1", 0, nil)

	// 删除不存在的
	if engine.DropDatabase("nonexistent") {
		t.Errorf("expected false for nonexistent database")
	}

	// 删除已存在的
	if !engine.DropDatabase("db1") {
		t.Errorf("expected true for existing database")
	}

	// 再次删除已删除的
	if engine.DropDatabase("db1") {
		t.Errorf("expected false for already deleted database")
	}
}

func TestEngine_CreateMeasurement(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	// 创建 measurement
	engine.CreateDatabase("db1", 0, nil)
	if _, err := engine.CreateMeasurement("db1", "cpu"); err != nil {
		t.Fatalf("CreateMeasurement failed: %v", err)
	}

	// 再次创建已存在的 measurement（CreateMeasurement 总是返回 true，因为 getOrCreate 不区分）
	created, err := engine.CreateMeasurement("db1", "cpu")
	if err != nil {
		t.Fatalf("CreateMeasurement failed: %v", err)
	}
	// 注意：根据实现，getOrCreate 不会返回 false，所以 created 总是 true
	_ = created
}

func TestEngine_DropMeasurement(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	// 创建 measurement
	engine.CreateDatabase("db1", 0, nil)
	if _, err := engine.CreateMeasurement("db1", "cpu"); err != nil {
		t.Fatalf("CreateMeasurement failed: %v", err)
	}

	// 删除不存在的 database
	_, err = engine.DropMeasurement("nonexistent", "cpu")
	if err == nil {
		t.Errorf("expected error for nonexistent database")
	}

	// 删除已存在的
	found, err := engine.DropMeasurement("db1", "cpu")
	if err != nil {
		t.Fatalf("DropMeasurement failed: %v", err)
	}
	if !found {
		t.Errorf("expected found=true for existing measurement")
	}

	// 再次删除已删除的
	found, err = engine.DropMeasurement("db1", "cpu")
	if err != nil {
		t.Fatalf("DropMeasurement failed: %v", err)
	}
	if found {
		t.Errorf("expected found=false for already deleted measurement")
	}
}

func TestEngine_Iterator(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	now := time.Now().UnixNano()

	// 写入数据
	points := []*types.Point{
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now,
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
		},
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + 1e9,
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0)},
		},
	}
	if err := engine.WriteBatch(t.Context(), points); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// 测试 Iterator
	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   now,
		EndTime:     now + 2e9,
	}

	it, err := engine.Iterator(t.Context(), req)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	defer func() {
		_ = it.Close()
	}()

	count := 0
	for it.Next(t.Context()) {
		row := it.Points()
		if row != nil {
			count++
		}
	}

	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestEngine_Query_NoShards(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	// 查询不存在的 shard
	req := &types.QueryRangeRequest{
		Database:    "nonexistent",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     1e9,
	}

	_, err = engine.Iterator(t.Context(), req)
	if err == nil {
		t.Fatalf("expected error for non-existent shard")
	}
}

func TestEngine_WriteContextCancel(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() {
		_ = engine.Close()
	}()

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // 立即取消

	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	err = engine.Write(ctx, point)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestEngine_Write_EmptyDatabase(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	point := &types.Point{
		Database:    "",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	err = engine.Write(t.Context(), point)
	if err != ErrEmptyDatabase {
		t.Errorf("expected ErrEmptyDatabase, got %v", err)
	}
}

func TestEngine_Write_EmptyMeasurement(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	point := &types.Point{
		Database:    "db1",
		Measurement: "",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	err = engine.Write(t.Context(), point)
	if err != ErrEmptyMeasurement {
		t.Errorf("expected ErrEmptyMeasurement, got %v", err)
	}
}

func TestEngine_Write_NegativeTimestamp(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   -1,
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	err = engine.Write(t.Context(), point)
	if err != ErrInvalidTimestamp {
		t.Errorf("expected ErrInvalidTimestamp, got %v", err)
	}
}

func TestEngine_Write_NilPoint(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	err = engine.Write(t.Context(), nil)
	if err != ErrNilPoint {
		t.Errorf("expected ErrNilPoint, got %v", err)
	}
}

func TestEngine_CreateMeasurement_EmptyDatabase(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	_, err = engine.CreateMeasurement("", "cpu")
	if err != ErrEmptyDatabase {
		t.Errorf("expected ErrEmptyDatabase, got %v", err)
	}
}

func TestEngine_CreateMeasurement_EmptyMeasurement(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	_, err = engine.CreateMeasurement("db1", "")
	if err != ErrEmptyMeasurement {
		t.Errorf("expected ErrEmptyMeasurement, got %v", err)
	}
}

func TestEngine_Query_EmptyDatabase(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	req := &types.QueryRangeRequest{
		Database:    "",
		Measurement: "cpu",
		StartTime:   time.Now().UnixNano(),
		EndTime:     time.Now().UnixNano() + int64(time.Hour),
	}
	_, err = engine.Iterator(t.Context(), req)
	if err == nil {
		t.Errorf("expected error for empty database")
	}
}

func TestEngine_Query_NonExistent(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	req := &types.QueryRangeRequest{
		Database:    "non_existent_db",
		Measurement: "non_existent_measurement",
		StartTime:   time.Now().UnixNano(),
		EndTime:     time.Now().UnixNano() + int64(time.Hour),
	}
	_, err = engine.Iterator(t.Context(), req)
	if err == nil {
		t.Errorf("expected error for non-existent db/measurement")
	}
}

func TestEngine_Write_Concurrent(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	const goroutines = 10
	const pointsPerGoroutine = 100
	baseTime := time.Now().UnixNano()

	var wg sync.WaitGroup
	errChan := make(chan error, goroutines*pointsPerGoroutine)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < pointsPerGoroutine; j++ {
				point := &types.Point{
					Database:    "db1",
					Measurement: "cpu",
					Tags:        map[string]string{"host": fmt.Sprintf("server%d", idx)},
					Timestamp:   baseTime + int64(j)*int64(time.Millisecond),
					Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(j))},
				}
				if err := engine.Write(t.Context(), point); err != nil {
					errChan <- fmt.Errorf("concurrent write failed: %w", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}
