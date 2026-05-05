// internal/storage/shard/shard_test.go
package shard

import (
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
	"codeberg.org/micro-ts/mts/types"
)

func TestShard_TimeRange(t *testing.T) {
	start := time.Now().UnixNano()
	end := start + int64(time.Hour)

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   start,
		EndTime:     end,
		Dir:         t.TempDir(),
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	})

	if s.StartTime() != start {
		t.Errorf("expected start %d, got %d", start, s.StartTime())
	}
	if s.EndTime() != end {
		t.Errorf("expected end %d, got %d", end, s.EndTime())
	}
}

func TestShard_ContainsTime(t *testing.T) {
	start := time.Now().UnixNano()
	end := start + int64(time.Hour)

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   start,
		EndTime:     end,
		Dir:         t.TempDir(),
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	})

	if !s.ContainsTime(start) {
		t.Errorf("shard should contain time %d", start)
	}
	if s.ContainsTime(end) {
		t.Errorf("shard should not contain end time %d", end)
	}
	if s.ContainsTime(start - 1) {
		t.Errorf("shard should not contain time before start")
	}
}

func TestShard_Duration(t *testing.T) {
	start := time.Now().UnixNano()
	end := start + int64(time.Hour)

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   start,
		EndTime:     end,
		Dir:         t.TempDir(),
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	})

	if s.Duration() != time.Hour {
		t.Errorf("expected duration 1h, got %v", s.Duration())
	}
}

func TestShard_Read_MergesMemTableAndSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	})

	// 写入数据到 MemTable
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": float64(i)},
		}
		err := s.Write(p)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Flush 到 SSTable
	err := s.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 再写入一些数据到 MemTable
	for i := 10; i < 20; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": float64(i)},
		}
		err := s.Write(p)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Read 应该合并 MemTable 和 SSTable
	rows, err := s.Read(0, 20*1e9)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 20 {
		t.Errorf("expected 20 rows, got %d", len(rows))
	}

	_ = s.Close()
}

// TestShard_WriteWithWAL 测试 WAL 集成
func TestShard_WriteWithWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建 mock MetaStore
	metaStore := measurement.NewMeasurementMetaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
	})

	// 写入数据
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]any{"usage": float64(85.5)},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 关闭 Shard 以确保 WAL 数据刷到磁盘
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证 WAL 写入
	points, err := ReplayWAL(filepath.Join(tmpDir, "wal"))
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}
	if len(points) != 1 {
		t.Errorf("expected 1 point in WAL, got %d", len(points))
	}
}
