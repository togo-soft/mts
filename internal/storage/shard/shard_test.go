// internal/storage/shard/shard_test.go
package shard

import (
	"testing"
	"time"

	"micro-ts/internal/types"
)

func TestShard_TimeRange(t *testing.T) {
	start := time.Now().UnixNano()
	end := start + int64(time.Hour)

	s := NewShard("db1", "cpu", start, end, t.TempDir())

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

	s := NewShard("db1", "cpu", start, end, t.TempDir())

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

	s := NewShard("db1", "cpu", start, end, t.TempDir())

	if s.Duration() != time.Hour {
		t.Errorf("expected duration 1h, got %v", s.Duration())
	}
}

func TestShard_Flush(t *testing.T) {
	tmpDir := t.TempDir()

	s := NewShard("db1", "cpu", 0, time.Hour.Nanoseconds(), tmpDir)
	defer func() {
		_ = s.Close()
	}()

	// 写入数据
	for i := 0; i < 100; i++ {
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

	// 验证写入后 MemTable 有数据
	if s.memTable.Count() != 100 {
		t.Errorf("expected memTable count 100, got %d", s.memTable.Count())
	}

	// 手动 flush
	err := s.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 验证 MemTable 已清空
	if s.memTable.Count() != 0 {
		t.Errorf("expected memTable count 0, got %d", s.memTable.Count())
	}
}
