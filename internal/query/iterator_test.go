package query

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"micro-ts/internal/storage/measurement"
	"micro-ts/internal/storage/shard"
	"micro-ts/types"
)

func TestQueryIterator_EmptyShardList(t *testing.T) {
	ctx := context.Background()
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   1000,
	}

	iter := NewQueryIterator(ctx, nil, req)

	// 空 shard 列表，Next 应该返回 false
	if iter.Next() {
		t.Error("expected Next() to return false for empty shard list")
	}
}

func TestQueryIterator_SingleShardBasic(t *testing.T) {
	dir := t.TempDir()

	// 创建 Shard
	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	s := shard.NewShard("db", "cpu", 0, 3600*1e9, shardDir, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())

	// 写入 MemTable 数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100), "field2": float64(10.5)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]any{"field1": int64(200), "field2": float64(20.5)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]any{"field1": int64(300), "field2": float64(30.5)}},
	}
	for _, p := range points {
		if err := s.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	ctx := context.Background()
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   4000,
	}

	iter := NewQueryIterator(ctx, []*shard.Shard{s}, req)

	// 收集所有结果
	var got []*types.PointRow
	for iter.Next() {
		got = append(got, iter.Points())
	}

	if len(got) != len(points) {
		t.Errorf("expected %d rows, got %d", len(points), len(got))
	}

	for i, row := range got {
		if row.Timestamp != points[i].Timestamp {
			t.Errorf("row[%d] timestamp: expected %d, got %d", i, points[i].Timestamp, row.Timestamp)
		}
	}
}

func TestQueryIterator_MultiShardMergeSort(t *testing.T) {
	dir := t.TempDir()

	// 创建两个 Shard
	shardDir0 := filepath.Join(dir, "shard0")
	shardDir1 := filepath.Join(dir, "shard1")
	if err := os.MkdirAll(shardDir0, 0700); err != nil {
		t.Fatalf("failed to create shard0 dir: %v", err)
	}
	if err := os.MkdirAll(shardDir1, 0700); err != nil {
		t.Fatalf("failed to create shard1 dir: %v", err)
	}

	s0 := shard.NewShard("db", "cpu", 0, 3600*1e9, shardDir0, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())
	s1 := shard.NewShard("db", "cpu", 3600*1e9, 7200*1e9, shardDir1, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())

	// Shard0: 1000, 3000
	points0 := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]any{"field1": int64(300)}},
	}
	for _, p := range points0 {
		if err := s0.Write(p); err != nil {
			t.Fatalf("failed to write point to shard0: %v", err)
		}
	}

	// Shard1: 4000, 5000
	points1 := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 4000, Fields: map[string]any{"field1": int64(400)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 5000, Fields: map[string]any{"field1": int64(500)}},
	}
	for _, p := range points1 {
		if err := s1.Write(p); err != nil {
			t.Fatalf("failed to write point to shard1: %v", err)
		}
	}

	ctx := context.Background()
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   6000,
	}

	iter := NewQueryIterator(ctx, []*shard.Shard{s0, s1}, req)

	// 期望按时间顺序: 1000, 3000, 4000, 5000
	expected := []int64{1000, 3000, 4000, 5000}

	var got []*types.PointRow
	for iter.Next() {
		got = append(got, iter.Points())
	}

	if len(got) != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), len(got))
	}

	for i, row := range got {
		if row.Timestamp != expected[i] {
			t.Errorf("row[%d] timestamp: expected %d, got %d", i, expected[i], row.Timestamp)
		}
	}
}

func TestQueryIterator_TagFiltering(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	s := shard.NewShard("db", "cpu", 0, 3600*1e9, shardDir, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())

	// 写入不同 tag 的数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1", "region": "us"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
		{Tags: map[string]string{"host": "server2", "region": "us"}, Timestamp: 2000, Fields: map[string]any{"field1": int64(200)}},
		{Tags: map[string]string{"host": "server1", "region": "eu"}, Timestamp: 3000, Fields: map[string]any{"field1": int64(300)}},
		{Tags: map[string]string{"host": "server2", "region": "eu"}, Timestamp: 4000, Fields: map[string]any{"field1": int64(400)}},
	}
	for _, p := range points {
		if err := s.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	ctx := context.Background()
	// 只查询 region=us 的数据
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   5000,
		Tags:      map[string]string{"region": "us"},
	}

	iter := NewQueryIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next() {
		got = append(got, iter.Points())
	}

	// 期望只返回 region=us 的数据
	if len(got) != 2 {
		t.Errorf("expected 2 rows, got %d", len(got))
	}

	for _, row := range got {
		if row.Tags["region"] != "us" {
			t.Errorf("expected region=us, got region=%s", row.Tags["region"])
		}
	}
}

func TestQueryIterator_FieldProjection(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	s := shard.NewShard("db", "cpu", 0, 3600*1e9, shardDir, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())

	// 写入包含多个字段的数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100), "field2": float64(10.5), "field3": "text"}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]any{"field1": int64(200), "field2": float64(20.5), "field3": "text2"}},
	}
	for _, p := range points {
		if err := s.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	ctx := context.Background()
	// 只查询 field1 和 field2
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   3000,
		Fields:    []string{"field1", "field2"},
	}

	iter := NewQueryIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next() {
		got = append(got, iter.Points())
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}

	// 验证 field3 被过滤掉
	for _, row := range got {
		if _, ok := row.Fields["field3"]; ok {
			t.Error("expected field3 to be filtered out")
		}
		if _, ok := row.Fields["field1"]; !ok {
			t.Error("expected field1 to be present")
		}
		if _, ok := row.Fields["field2"]; !ok {
			t.Error("expected field2 to be present")
		}
	}
}

func TestQueryIterator_OffsetSkip(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	s := shard.NewShard("db", "cpu", 0, 3600*1e9, shardDir, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())

	// 写入 5 条数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]any{"field1": int64(200)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]any{"field1": int64(300)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 4000, Fields: map[string]any{"field1": int64(400)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 5000, Fields: map[string]any{"field1": int64(500)}},
	}
	for _, p := range points {
		if err := s.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	ctx := context.Background()
	// 跳过前 2 条
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   6000,
		Offset:    2,
	}

	iter := NewQueryIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next() {
		got = append(got, iter.Points())
	}

	// 期望返回后 3 条: 3000, 4000, 5000
	if len(got) != 3 {
		t.Errorf("expected 3 rows, got %d", len(got))
	}

	expected := []int64{3000, 4000, 5000}
	for i, row := range got {
		if row.Timestamp != expected[i] {
			t.Errorf("row[%d] timestamp: expected %d, got %d", i, expected[i], row.Timestamp)
		}
	}
}

func TestQueryIterator_LimitRestriction(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	s := shard.NewShard("db", "cpu", 0, 3600*1e9, shardDir, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())

	// 写入 5 条数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]any{"field1": int64(200)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]any{"field1": int64(300)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 4000, Fields: map[string]any{"field1": int64(400)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 5000, Fields: map[string]any{"field1": int64(500)}},
	}
	for _, p := range points {
		if err := s.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	ctx := context.Background()
	// 只返回前 3 条
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   6000,
		Limit:     3,
	}

	iter := NewQueryIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next() {
		got = append(got, iter.Points())
	}

	if len(got) != 3 {
		t.Errorf("expected 3 rows, got %d", len(got))
	}

	expected := []int64{1000, 2000, 3000}
	for i, row := range got {
		if row.Timestamp != expected[i] {
			t.Errorf("row[%d] timestamp: expected %d, got %d", i, expected[i], row.Timestamp)
		}
	}
}

func TestQueryIterator_OffsetAndLimit(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	s := shard.NewShard("db", "cpu", 0, 3600*1e9, shardDir, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())

	// 写入 10 条数据
	points := make([]*types.Point, 10)
	for i := 0; i < 10; i++ {
		points[i] = &types.Point{
			Tags:      map[string]string{"host": "server1"},
			Timestamp: int64((i + 1) * 1000),
			Fields:    map[string]any{"field1": int64((i + 1) * 100)},
		}
		if err := s.Write(points[i]); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	ctx := context.Background()
	// 跳过前 2 条，只返回接下来的 3 条
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   11000,
		Offset:    2,
		Limit:     3,
	}

	iter := NewQueryIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next() {
		got = append(got, iter.Points())
	}

	if len(got) != 3 {
		t.Errorf("expected 3 rows, got %d", len(got))
	}

	// 期望: 3000, 4000, 5000 (偏移2后取3个)
	expected := []int64{3000, 4000, 5000}
	for i, row := range got {
		if row.Timestamp != expected[i] {
			t.Errorf("row[%d] timestamp: expected %d, got %d", i, expected[i], row.Timestamp)
		}
	}
}

func TestQueryIterator_Close(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	s := shard.NewShard("db", "cpu", 0, 3600*1e9, shardDir, measurement.NewMeasurementMetaStore(), shard.DefaultMemTableConfig())

	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
	}
	for _, p := range points {
		if err := s.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	ctx := context.Background()
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   2000,
	}

	iter := NewQueryIterator(ctx, []*shard.Shard{s}, req)

	// Close 应该成功
	if err := iter.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Close 后 Next 应该返回 false
	if iter.Next() {
		t.Error("Next() should return false after Close()")
	}
}
