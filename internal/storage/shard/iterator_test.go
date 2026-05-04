// internal/storage/shard/iterator_test.go
package shard

import (
	"os"
	"path/filepath"
	"testing"

	"micro-ts/internal/types"
)

func TestShardIterator_MemTableOnly(t *testing.T) {
	// 创建临时目录
	dir := t.TempDir()

	// 创建 Shard
	shard := NewShard("db", "cpu", 0, 3600*1e9, dir, nil, DefaultMemTableConfig())

	// 写入 MemTable 数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]any{"field1": int64(200)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]any{"field1": int64(300)}},
	}

	for _, p := range points {
		if err := shard.memTable.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	// 创建迭代器（0, 0 表示不过滤时间）
	iter := NewShardIterator(shard, 0, 0)

	// 验证顺序
	var got []*types.PointRow
	for {
		row := iter.Next()
		if row == nil {
			break
		}
		got = append(got, row)
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

func TestShardIterator_SSTableOnly(t *testing.T) {
	// 创建临时目录
	dir := t.TempDir()

	// 准备 SSTable 数据
	sstDir := filepath.Join(dir, "sst")
	if err := os.MkdirAll(filepath.Join(sstDir, "data", "fields"), 0700); err != nil {
		t.Fatalf("failed to create sst dir: %v", err)
	}

	// 写入 timestamps
	timestamps := []int64{1000, 2000, 3000}
	tsFile, err := os.Create(filepath.Join(sstDir, "data", "_timestamps.bin"))
	if err != nil {
		t.Fatalf("failed to create timestamps file: %v", err)
	}
	for _, ts := range timestamps {
		var buf [8]byte
		buf[0] = byte(ts >> 56)
		buf[1] = byte(ts >> 48)
		buf[2] = byte(ts >> 40)
		buf[3] = byte(ts >> 32)
		buf[4] = byte(ts >> 24)
		buf[5] = byte(ts >> 16)
		buf[6] = byte(ts >> 8)
		buf[7] = byte(ts)
		if _, err := tsFile.Write(buf[:]); err != nil {
			tsFile.Close()
			t.Fatalf("failed to write timestamp: %v", err)
		}
	}
	if err := tsFile.Close(); err != nil {
		t.Fatalf("failed to close timestamps file: %v", err)
	}

	// 写入字段数据
	fieldFile, err := os.Create(filepath.Join(sstDir, "data", "fields", "field1.bin"))
	if err != nil {
		t.Fatalf("failed to create field file: %v", err)
	}
	for i := 0; i < len(timestamps); i++ {
		var buf [8]byte
		val := int64(100 + i*100)
		buf[0] = byte(val >> 56)
		buf[1] = byte(val >> 48)
		buf[2] = byte(val >> 40)
		buf[3] = byte(val >> 32)
		buf[4] = byte(val >> 24)
		buf[5] = byte(val >> 16)
		buf[6] = byte(val >> 8)
		buf[7] = byte(val)
		if _, err := fieldFile.Write(buf[:]); err != nil {
			fieldFile.Close()
			t.Fatalf("failed to write field: %v", err)
		}
	}
	if err := fieldFile.Close(); err != nil {
		t.Fatalf("failed to close field file: %v", err)
	}

	// 创建 Shard
	shard := NewShard("db", "cpu", 0, 3600*1e9, sstDir, nil, DefaultMemTableConfig())

	// 创建迭代器（0, 0 表示不过滤时间）
	iter := NewShardIterator(shard, 0, 0)

	// 验证顺序
	var got []*types.PointRow
	for {
		row := iter.Next()
		if row == nil {
			break
		}
		got = append(got, row)
	}

	if len(got) != len(timestamps) {
		t.Errorf("expected %d rows, got %d", len(timestamps), len(got))
	}

	for i, row := range got {
		if row.Timestamp != timestamps[i] {
			t.Errorf("row[%d] timestamp: expected %d, got %d", i, timestamps[i], row.Timestamp)
		}
	}
}

func TestShardIterator_BothMemTableAndSSTable(t *testing.T) {
	// 创建临时目录
	dir := t.TempDir()

	// 准备 SSTable 数据
	sstDir := filepath.Join(dir, "sst")
	if err := os.MkdirAll(filepath.Join(sstDir, "data", "fields"), 0700); err != nil {
		t.Fatalf("failed to create sst dir: %v", err)
	}

	// 写入 timestamps (SSTable: 2000, 4000)
	sstTimestamps := []int64{2000, 4000}
	tsFile, err := os.Create(filepath.Join(sstDir, "data", "_timestamps.bin"))
	if err != nil {
		t.Fatalf("failed to create timestamps file: %v", err)
	}
	for _, ts := range sstTimestamps {
		var buf [8]byte
		buf[0] = byte(ts >> 56)
		buf[1] = byte(ts >> 48)
		buf[2] = byte(ts >> 40)
		buf[3] = byte(ts >> 32)
		buf[4] = byte(ts >> 24)
		buf[5] = byte(ts >> 16)
		buf[6] = byte(ts >> 8)
		buf[7] = byte(ts)
		if _, err := tsFile.Write(buf[:]); err != nil {
			tsFile.Close()
			t.Fatalf("failed to write timestamp: %v", err)
		}
	}
	if err := tsFile.Close(); err != nil {
		t.Fatalf("failed to close timestamps file: %v", err)
	}

	// 写入字段数据
	fieldFile, err := os.Create(filepath.Join(sstDir, "data", "fields", "field1.bin"))
	if err != nil {
		t.Fatalf("failed to create field file: %v", err)
	}
	for i := 0; i < len(sstTimestamps); i++ {
		var buf [8]byte
		val := int64(200 + i*200) // 200, 400
		buf[0] = byte(val >> 56)
		buf[1] = byte(val >> 48)
		buf[2] = byte(val >> 40)
		buf[3] = byte(val >> 32)
		buf[4] = byte(val >> 24)
		buf[5] = byte(val >> 16)
		buf[6] = byte(val >> 8)
		buf[7] = byte(val)
		if _, err := fieldFile.Write(buf[:]); err != nil {
			fieldFile.Close()
			t.Fatalf("failed to write field: %v", err)
		}
	}
	if err := fieldFile.Close(); err != nil {
		t.Fatalf("failed to close field file: %v", err)
	}

	// 创建 Shard
	shard := NewShard("db", "cpu", 0, 3600*1e9, sstDir, nil, DefaultMemTableConfig())

	// 写入 MemTable 数据 (MemTable: 1000, 3000)
	memPoints := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 3000, Fields: map[string]any{"field1": int64(300)}},
	}
	for _, p := range memPoints {
		if err := shard.memTable.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	// 创建迭代器（0, 0 表示不过滤时间）
	iter := NewShardIterator(shard, 0, 0)

	// 期望顺序: 1000, 2000, 3000, 4000
	expected := []int64{1000, 2000, 3000, 4000}

	var got []*types.PointRow
	for {
		row := iter.Next()
		if row == nil {
			break
		}
		got = append(got, row)
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

func TestShardIterator_EqualTimestamps(t *testing.T) {
	// 创建临时目录
	dir := t.TempDir()

	// 准备 SSTable 数据
	sstDir := filepath.Join(dir, "sst")
	if err := os.MkdirAll(filepath.Join(sstDir, "data", "fields"), 0700); err != nil {
		t.Fatalf("failed to create sst dir: %v", err)
	}

	// 写入 timestamps (SSTable: 1000, 3000)
	sstTimestamps := []int64{1000, 3000}
	tsFile, err := os.Create(filepath.Join(sstDir, "data", "_timestamps.bin"))
	if err != nil {
		t.Fatalf("failed to create timestamps file: %v", err)
	}
	for _, ts := range sstTimestamps {
		var buf [8]byte
		buf[0] = byte(ts >> 56)
		buf[1] = byte(ts >> 48)
		buf[2] = byte(ts >> 40)
		buf[3] = byte(ts >> 32)
		buf[4] = byte(ts >> 24)
		buf[5] = byte(ts >> 16)
		buf[6] = byte(ts >> 8)
		buf[7] = byte(ts)
		if _, err := tsFile.Write(buf[:]); err != nil {
			tsFile.Close()
			t.Fatalf("failed to write timestamp: %v", err)
		}
	}
	if err := tsFile.Close(); err != nil {
		t.Fatalf("failed to close timestamps file: %v", err)
	}

	// 写入字段数据
	fieldFile, err := os.Create(filepath.Join(sstDir, "data", "fields", "field1.bin"))
	if err != nil {
		t.Fatalf("failed to create field file: %v", err)
	}
	for i := 0; i < len(sstTimestamps); i++ {
		var buf [8]byte
		val := int64(200 + i*200) // 200, 400
		buf[0] = byte(val >> 56)
		buf[1] = byte(val >> 48)
		buf[2] = byte(val >> 40)
		buf[3] = byte(val >> 32)
		buf[4] = byte(val >> 24)
		buf[5] = byte(val >> 16)
		buf[6] = byte(val >> 8)
		buf[7] = byte(val)
		if _, err := fieldFile.Write(buf[:]); err != nil {
			fieldFile.Close()
			t.Fatalf("failed to write field: %v", err)
		}
	}
	if err := fieldFile.Close(); err != nil {
		t.Fatalf("failed to close field file: %v", err)
	}

	// 创建 Shard
	shard := NewShard("db", "cpu", 0, 3600*1e9, sstDir, nil, DefaultMemTableConfig())

	// 写入 MemTable 数据 (MemTable: 1000, 2000) - 1000 与 SSTable 相同
	memPoints := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]any{"field1": int64(200)}},
	}
	for _, p := range memPoints {
		if err := shard.memTable.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	// 创建迭代器（0, 0 表示不过滤时间）
	iter := NewShardIterator(shard, 0, 0)

	// 当 timestamp 相等时，SSTable 优先（因为在 else 分支）
	// 期望顺序: 1000(SSTable), 1000(MemTable), 2000, 3000
	var got []*types.PointRow
	for {
		row := iter.Next()
		if row == nil {
			break
		}
		got = append(got, row)
	}

	// 验证数量
	if len(got) != 4 {
		t.Errorf("expected 4 rows, got %d", len(got))
	}

	// 验证顺序：SSTable 1000 在前，MemTable 1000 在后
	if got[0].Timestamp != 1000 {
		t.Errorf("first row timestamp: expected 1000, got %d", got[0].Timestamp)
	}
	if got[1].Timestamp != 1000 {
		t.Errorf("second row timestamp: expected 1000, got %d", got[1].Timestamp)
	}
	if got[2].Timestamp != 2000 {
		t.Errorf("third row timestamp: expected 2000, got %d", got[2].Timestamp)
	}
	if got[3].Timestamp != 3000 {
		t.Errorf("fourth row timestamp: expected 3000, got %d", got[3].Timestamp)
	}
}

func TestShardIterator_Current(t *testing.T) {
	// 创建临时目录
	dir := t.TempDir()

	// 创建 Shard
	shard := NewShard("db", "cpu", 0, 3600*1e9, dir, nil, DefaultMemTableConfig())

	// 写入 MemTable 数据
	points := []*types.Point{
		{Tags: map[string]string{"host": "server1"}, Timestamp: 1000, Fields: map[string]any{"field1": int64(100)}},
		{Tags: map[string]string{"host": "server1"}, Timestamp: 2000, Fields: map[string]any{"field1": int64(200)}},
	}
	for _, p := range points {
		if err := shard.memTable.Write(p); err != nil {
			t.Fatalf("failed to write point: %v", err)
		}
	}

	// 创建迭代器（0, 0 表示不过滤时间）
	iter := NewShardIterator(shard, 0, 0)

	// 初始状态 Current 应该返回第一条
	current := iter.Current()
	if current == nil {
		t.Fatal("expected current to return first row")
	}
	if current.Timestamp != 1000 {
		t.Errorf("current timestamp: expected 1000, got %d", current.Timestamp)
	}

	// Next 后再 Current 应该返回第二条
	iter.Next()
	current = iter.Current()
	if current == nil {
		t.Fatal("expected current to return second row")
	}
	if current.Timestamp != 2000 {
		t.Errorf("current timestamp: expected 2000, got %d", current.Timestamp)
	}
}

func TestShardIterator_Empty(t *testing.T) {
	// 创建临时目录
	dir := t.TempDir()

	// 创建 Shard（无数据）
	shard := NewShard("db", "cpu", 0, 3600*1e9, dir, nil, DefaultMemTableConfig())

	// 创建迭代器（0, 0 表示不过滤时间）
	iter := NewShardIterator(shard, 0, 0)

	// Next 应该返回 nil
	row := iter.Next()
	if row != nil {
		t.Errorf("expected nil row, got %v", row)
	}

	// Current 应该返回 nil
	current := iter.Current()
	if current != nil {
		t.Errorf("expected nil current, got %v", current)
	}
}
