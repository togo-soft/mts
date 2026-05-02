// internal/storage/shard/shard_test.go
package shard

import (
    "testing"
    "time"
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
    if s.ContainsTime(start-1) {
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
