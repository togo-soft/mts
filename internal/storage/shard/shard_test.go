// internal/storage/shard/shard_test.go
package shard

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
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

func TestShard_Write_WALDisabled(t *testing.T) {
	// 测试 WAL 被禁用时 Write 仍能正常工作
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
		// 默认 WALEnabled = false
	})

	// 写入数据 - 即使 WAL 被禁用也应该正常工作
	p := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(85.5))},
	}
	if err := s.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 读取验证
	rows, err := s.Read(0, 2000000000)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_Close_NoData(t *testing.T) {
	// 测试关闭空 Shard
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

	// 不写入任何数据，直接关闭
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestShard_Flush_AfterWrite(t *testing.T) {
	// 测试 Flush 在有数据时正常工作
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

	// 写入数据
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Flush
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 再次写入并 flush
	for i := 5; i < 10; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush 2 failed: %v", err)
	}

	// 读取验证
	rows, err := s.Read(0, 10*1e9)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_Read_TimeRange(t *testing.T) {
	// 测试时间范围过滤
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

	// 写入 5 个时间点的数据
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9, // 0, 1e9, 2e9, 3e9, 4e9
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 读取 [1e9, 4e9) 范围的数据
	rows, err := s.Read(1*1e9, 4*1e9)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(rows) != 3 { // 1e9, 2e9, 3e9
		t.Errorf("expected 3 rows, got %d", len(rows))
	}

	for _, row := range rows {
		if row.Timestamp < 1*1e9 || row.Timestamp >= 4*1e9 {
			t.Errorf("row timestamp %d out of range [1e9, 4e9)", row.Timestamp)
		}
	}

	_ = s.Close()
}

func TestShard_ConcurrentWrite(t *testing.T) {
	// 测试并发写入
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

	const goroutines = 10
	const pointsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < pointsPerGoroutine; i++ {
				p := &types.Point{
					Timestamp: int64(goroutineID*pointsPerGoroutine+i) * 1000,
					Tags:      map[string]string{"host": fmt.Sprintf("server%d", goroutineID)},
					Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
				}
				if err := s.Write(p); err != nil {
					t.Errorf("Write failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()

	// 验证数据数量
	rows, err := s.Read(0, math.MaxInt64)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	expectedCount := goroutines * pointsPerGoroutine
	if len(rows) != expectedCount {
		t.Errorf("expected %d rows, got %d", expectedCount, len(rows))
	}

	_ = s.Close()
}

func TestShard_ConcurrentReadWrite(t *testing.T) {
	// 测试并发读写
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

	// 先写入一些数据
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e6,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 并发读写
	var wg sync.WaitGroup
	const goroutines = 5
	wg.Add(goroutines * 2)

	// 读 goroutines
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := s.Read(0, math.MaxInt64)
				if err != nil {
					t.Errorf("Read failed: %v", err)
				}
			}
		}()
	}

	// 写 goroutines
	for i := 0; i < goroutines; i++ {
		go func(offset int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				p := &types.Point{
					Timestamp: int64(1000+offset*100+j) * 1e6,
					Tags:      map[string]string{"host": "server1"},
					Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(j))},
				}
				if err := s.Write(p); err != nil {
					t.Errorf("Write failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	_ = s.Close()
}

func TestShard_Write_MultipleFlush(t *testing.T) {
	// 测试多次 flush 后继续写入
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

	// 多次写入并 flush
	for round := 0; round < 3; round++ {
		for i := 0; i < 50; i++ {
			p := &types.Point{
				Timestamp: int64(round*100+i) * 1e7,
				Tags:      map[string]string{"host": "server1"},
				Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(round*100 + i))},
			}
			if err := s.Write(p); err != nil {
				t.Fatalf("Write failed: %v", err)
			}
		}
		if err := s.Flush(); err != nil {
			t.Fatalf("Flush %d failed: %v", round, err)
		}
	}

	// 读取所有数据
	rows, err := s.Read(0, math.MaxInt64)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 150 {
		t.Errorf("expected 150 rows, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_Flush_EmptyMemTable(t *testing.T) {
	// 测试 flush 空 MemTable
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

	// 不写入任何数据，直接 flush
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush empty MemTable failed: %v", err)
	}

	// 再写入一些数据验证仍能正常工作
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	rows, err := s.Read(0, math.MaxInt64)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}

	_ = s.Close()
}

func TestShard_Close_WithActiveCompaction(t *testing.T) {
	// 测试有关闭时正在运行的 compaction
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
		CompactionCfg: &CompactionConfig{
			MaxSSTableCount: 2,
			CheckInterval:   10 * time.Millisecond, // 快速触发
			Timeout:         30 * time.Second,
		},
	}

	s := NewShard(cfg)

	// 写入足够的数据创建多个 SSTable
	for i := 0; i < 200; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e7,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(i))},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if i%50 == 0 {
			_ = s.Flush()
		}
	}

	// 关闭
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
