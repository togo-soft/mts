package query

import (
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// writePointsToShard 将 Point 列表写入 Shard 的 SSTable（替代已删除的 Shard.Write）。
func writePointsToShard(t testing.TB, s *shard.Shard, ss shard.SeriesStore, db, meas string, points []*types.Point) {
	t.Helper()
	memPoints := make([]types.MemPoint, len(points))
	for i, p := range points {
		sid, err := ss.AllocateSID(db, meas, p.Tags)
		if err != nil {
			t.Fatalf("AllocateSID failed: %v", err)
		}
		memPoints[i] = types.PointToMemPoint(p, sid)
	}
	_, _, _, _, err := s.WriteSSTable(memPoints)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}
}

func TestIterator_EmptyShardList(t *testing.T) {
	ctx := t.Context()
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   1000,
	}

	iter := NewIterator(ctx, nil, req)

	// 空 shard 列表，Next 应该返回 false
	if iter.Next(ctx) {
		t.Error("expected Next() to return false for empty shard list")
	}
}

func TestIterator_SingleShardBasic(t *testing.T) {
	dir := t.TempDir()

	// 创建 Shard
	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	seriesStore := metadata.NewSimpleSeriesStore()
	s := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     3600 * 1e9,
		Dir:         shardDir,
		SeriesStore: seriesStore,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// 写入 SSTable 数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(100)), "field2": types.NewFieldValue(float64(10.5))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(200)), "field2": types.NewFieldValue(float64(20.5))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(300)), "field2": types.NewFieldValue(float64(30.5))}},
	}
	writePointsToShard(t, s, seriesStore, "db", "cpu", points)

	ctx := t.Context()
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   4000,
	}

	iter := NewIterator(ctx, []*shard.Shard{s}, req)

	// 收集所有结果
	var got []*types.PointRow
	for iter.Next(ctx) {
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

func TestIterator_MultiShardMergeSort(t *testing.T) {
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

	s0series := metadata.NewSimpleSeriesStore()
	s0 := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     3600 * 1e9,
		Dir:         shardDir0,
		SeriesStore: s0series,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})
	s1series := metadata.NewSimpleSeriesStore()
	s1 := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   3600 * 1e9,
		EndTime:     7200 * 1e9,
		Dir:         shardDir1,
		SeriesStore: s1series,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// Shard0: 1000, 3000
	points0 := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(100))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(300))}},
	}
	writePointsToShard(t, s0, s0series, "db", "cpu", points0)

	// Shard1: 4000, 5000
	points1 := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 4000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(400))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 5000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(500))}},
	}
	writePointsToShard(t, s1, s1series, "db", "cpu", points1)

	ctx := t.Context()
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   6000,
	}

	iter := NewIterator(ctx, []*shard.Shard{s0, s1}, req)

	// 期望按时间顺序: 1000, 3000, 4000, 5000
	expected := []int64{1000, 3000, 4000, 5000}

	var got []*types.PointRow
	for iter.Next(ctx) {
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

func TestIterator_TagFiltering(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	seriesStore := metadata.NewSimpleSeriesStore()
	s := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     3600 * 1e9,
		Dir:         shardDir,
		SeriesStore: seriesStore,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// 写入不同 tag 的数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1", "region": "us"}, Timestamp: 1000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(100))}},
		{Tags: map[string]string{"host": "server2", "region": "us"}, Timestamp: 2000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(200))}},
		{Tags: map[string]string{"host": "server1", "region": "eu"}, Timestamp: 3000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(300))}},
		{Tags: map[string]string{"host": "server2", "region": "eu"}, Timestamp: 4000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(400))}},
	}
	writePointsToShard(t, s, seriesStore, "db", "cpu", points)

	ctx := t.Context()
	// 只查询 region=us 的数据
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   5000,
		Tags:      map[string]string{"region": "us"},
	}

	iter := NewIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next(ctx) {
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

func TestIterator_FieldProjection(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	seriesStore := metadata.NewSimpleSeriesStore()
	s := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     3600 * 1e9,
		Dir:         shardDir,
		SeriesStore: seriesStore,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// 写入包含多个字段的数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(100)), "field2": types.NewFieldValue(float64(10.5)), "field3": types.NewFieldValue("text")}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(200)), "field2": types.NewFieldValue(float64(20.5)), "field3": types.NewFieldValue("text2")}},
	}
	writePointsToShard(t, s, seriesStore, "db", "cpu", points)

	ctx := t.Context()
	// 只查询 field1 和 field2
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   3000,
		Fields:    []string{"field1", "field2"},
	}

	iter := NewIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next(ctx) {
		got = append(got, iter.Points())
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}

	// 验证 field3 被过滤掉
	for _, row := range got {
		if row.GetFieldValue("field3") != nil {
			t.Error("expected field3 to be filtered out")
		}
		if row.GetFieldValue("field1") == nil {
			t.Error("expected field1 to be present")
		}
		if row.GetFieldValue("field2") == nil {
			t.Error("expected field2 to be present")
		}
	}
}

func TestIterator_OffsetSkip(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	seriesStore := metadata.NewSimpleSeriesStore()
	s := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     3600 * 1e9,
		Dir:         shardDir,
		SeriesStore: seriesStore,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// 写入 5 条数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(100))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(200))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(300))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 4000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(400))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 5000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(500))}},
	}
	writePointsToShard(t, s, seriesStore, "db", "cpu", points)

	ctx := t.Context()
	// 跳过前 2 条
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   6000,
		Offset:    2,
	}

	iter := NewIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next(ctx) {
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

func TestIterator_LimitRestriction(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	seriesStore := metadata.NewSimpleSeriesStore()
	s := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     3600 * 1e9,
		Dir:         shardDir,
		SeriesStore: seriesStore,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// 写入 5 条数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(100))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(200))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(300))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 4000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(400))}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 5000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(500))}},
	}
	writePointsToShard(t, s, seriesStore, "db", "cpu", points)

	ctx := t.Context()
	// 只返回前 3 条
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   6000,
		Limit:     3,
	}

	iter := NewIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next(ctx) {
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

func TestIterator_OffsetAndLimit(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	seriesStore := metadata.NewSimpleSeriesStore()
	s := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     3600 * 1e9,
		Dir:         shardDir,
		SeriesStore: seriesStore,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// 写入 10 条数据
	points := make([]*types.Point, 10)
	for i := 0; i < 10; i++ {
		points[i] = &types.Point{
			Tags:      map[string]string{"host": "server1"},
			Timestamp: int64((i + 1) * 1000),
			Fields:    map[string]*types.FieldValue{"field1": types.NewFieldValue(int64((i + 1) * 100))},
		}
	}
	writePointsToShard(t, s, seriesStore, "db", "cpu", points)

	ctx := t.Context()
	// 跳过前 2 条，只返回接下来的 3 条
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   11000,
		Offset:    2,
		Limit:     3,
	}

	iter := NewIterator(ctx, []*shard.Shard{s}, req)

	var got []*types.PointRow
	for iter.Next(ctx) {
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

func TestIterator_Close(t *testing.T) {
	dir := t.TempDir()

	shardDir := filepath.Join(dir, "shard0")
	if err := os.MkdirAll(shardDir, 0700); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	seriesStore := metadata.NewSimpleSeriesStore()
	s := shard.NewShard(shard.ShardConfig{
		DB:          "db",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     3600 * 1e9,
		Dir:         shardDir,
		SeriesStore: seriesStore,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]*types.FieldValue{"field1": types.NewFieldValue(int64(100))}},
	}
	writePointsToShard(t, s, seriesStore, "db", "cpu", points)

	ctx := t.Context()
	req := &types.QueryRangeRequest{
		StartTime: 0,
		EndTime:   2000,
	}

	iter := NewIterator(ctx, []*shard.Shard{s}, req)

	// Close 应该成功
	if err := iter.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Close 后 Next 应该返回 false
	if iter.Next(ctx) {
		t.Error("Next() should return false after Close()")
	}
}
