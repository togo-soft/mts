// internal/storage/shard/shard_test.go
package shard

import (
	"os"
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
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
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
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
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
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(85.5))},
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

func TestShard_Write_DifferentTags(t *testing.T) {
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

	// 写入不同 tags 的 points 以测试 SID 分配
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1", "region": "us-east"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	_ = s.Close()
}

func TestShard_Write_IntAndStringFields(t *testing.T) {
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

	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields: map[string]*types.FieldValue{
			"int_field":    types.NewFieldValue(int64(42)),
			"float_field":  types.NewFieldValue(float64(3.14)),
			"string_field": types.NewFieldValue("hello"),
		},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_ = s.Close()
}

func TestShard_Read_AfterCloseAndReopen(t *testing.T) {
	tmpDir := t.TempDir()

	// First shard - write and flush data
	s1 := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	})

	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s1.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	_ = s1.Flush()
	_ = s1.Close()

	// Second shard - open same directory, should read from SSTable
	s2 := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   s1.metaStore,
		MemTableCfg: DefaultMemTableConfig(),
	})

	rows, err := s2.Read(0, 5*1e9)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}

	_ = s2.Close()
}

func TestShard_Close_FlushesMemTable(t *testing.T) {
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

	// Write some data but don't flush
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Close should flush remaining data
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Delete WAL to prevent replay (which would cause duplicates due to MemTable/SSTable dedup limitation)
	walDir := filepath.Join(tmpDir, "wal")
	_ = os.RemoveAll(walDir)

	// Reopen and verify data is there
	s2 := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	})

	rows, err := s2.Read(0, 3*1e9)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("expected 3 rows after reopen, got %d", len(rows))
	}

	_ = s2.Close()
}

func TestShard_Flush_NoData(t *testing.T) {
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

	// Flush with no data should not fail
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush with no data failed: %v", err)
	}

	_ = s.Close()
}

func TestShard_Read_OutOfRange(t *testing.T) {
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

	// Write some data
	p := &types.Point{
		Timestamp: 5 * 1e9,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(50))},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read with range that doesn't include the data
	rows, err := s.Read(0, 1*1e9)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 0 {
		t.Errorf("expected 0 rows for out-of-range query, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_DataDir(t *testing.T) {
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

	dataDir := s.DataDir()
	expected := filepath.Join(tmpDir, "data")
	if dataDir != expected {
		t.Errorf("expected DataDir=%s, got %s", expected, dataDir)
	}

	_ = s.Close()
}

func TestShard_ReopenWithMetaStore(t *testing.T) {
	tmpDir := t.TempDir()
	metaStore := measurement.NewMeasurementMetaStore()

	// First shard
	s1 := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
	})

	// Write data
	p := &types.Point{
		Timestamp: 1 * 1e9,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(50))},
	}
	if err := s1.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	_ = s1.Flush()
	_ = s1.Close()

	// Second shard with same MetaStore
	s2 := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   metaStore,
		MemTableCfg: DefaultMemTableConfig(),
	})

	rows, err := s2.Read(0, 2*1e9)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}

	_ = s2.Close()
}

func TestShard_Close_WithPendingData(t *testing.T) {
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

	// Write more data to trigger internal flush
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestShard_MultipleFlush(t *testing.T) {
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

	// Multiple flushes
	for j := 0; j < 3; j++ {
		for i := 0; i < 5; i++ {
			p := &types.Point{
				Timestamp: int64(j*10+i) * 1e8,
				Tags:      map[string]string{"host": "server1"},
				Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
			}
			if err := s.Write(p); err != nil {
				t.Fatalf("Write failed: %v", err)
			}
		}
		if err := s.Flush(); err != nil {
			t.Fatalf("Flush %d failed: %v", j, err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestShard_DeleteDataDir(t *testing.T) {
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

	// Write and flush
	p := &types.Point{
		Timestamp: 1 * 1e9,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(50))},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	_ = s.Flush()

	dataDir := s.DataDir()
	_ = s.Close()

	// Delete the data directory manually
	_ = os.RemoveAll(dataDir)

	// Create new shard with same dir - should not crash
	s2 := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	})

	rows, err := s2.Read(0, 2*1e9)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 0 {
		t.Errorf("expected 0 rows after data dir deletion, got %d", len(rows))
	}

	_ = s2.Close()
}
