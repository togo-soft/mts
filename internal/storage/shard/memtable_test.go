package shard

import (
	"testing"
	"time"

	"micro-ts/internal/types"
)

func TestMemTable_Write(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	p := &types.Point{
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]any{"usage": 85.5},
	}

	if err := m.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if m.Count() != 1 {
		t.Errorf("expected count 1, got %d", m.Count())
	}
}

func TestMemTable_SortKey(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	now := time.Now().UnixNano()
	p1 := &types.Point{Measurement: "cpu", Timestamp: now + 100}
	p2 := &types.Point{Measurement: "cpu", Timestamp: now}
	p3 := &types.Point{Measurement: "cpu", Timestamp: now + 200}

	if err := m.Write(p2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := m.Write(p1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := m.Write(p3); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 验证排序
	if m.entries[0].Point.Timestamp != now {
		t.Errorf("expected first timestamp %d, got %d", now, m.entries[0].Point.Timestamp)
	}
	if m.entries[1].Point.Timestamp != now+100 {
		t.Errorf("expected second timestamp %d, got %d", now+100, m.entries[1].Point.Timestamp)
	}
	if m.entries[2].Point.Timestamp != now+200 {
		t.Errorf("expected third timestamp %d, got %d", now+200, m.entries[2].Point.Timestamp)
	}
}

func TestMemTable_ShouldFlush(t *testing.T) {
	cfg := MemTableConfig{MaxSize: 100, MaxCount: 0, IdleDuration: 0}
	m := NewMemTable(cfg)

	p := &types.Point{
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]any{"usage": 85.5},
	}

	// 写入一些数据直到应该 flush
	for !m.ShouldFlush() {
		if err := m.Write(p); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if !m.ShouldFlush() {
		t.Errorf("expected ShouldFlush to return true")
	}
}
