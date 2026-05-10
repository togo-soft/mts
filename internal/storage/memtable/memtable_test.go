package memtable

import (
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

func TestMemTable_Write(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	p := &types.Point{
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}

	if err := m.Write(p, 0); err != nil {
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

	if err := m.Write(p2, 0); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := m.Write(p1, 0); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := m.Write(p3, 0); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// entries is unexported, verify via iterator
	it := m.Iterator()
	if !it.Next() {
		t.Fatal("expected first entry")
	}
	if it.Point().Timestamp != now {
		t.Errorf("expected first timestamp %d, got %d", now, it.Point().Timestamp)
	}
	if !it.Next() {
		t.Fatal("expected second entry")
	}
	if it.Point().Timestamp != now+100 {
		t.Errorf("expected second timestamp %d, got %d", now+100, it.Point().Timestamp)
	}
	if !it.Next() {
		t.Fatal("expected third entry")
	}
	if it.Point().Timestamp != now+200 {
		t.Errorf("expected third timestamp %d, got %d", now+200, it.Point().Timestamp)
	}
}

func TestMemTable_ShouldFlush(t *testing.T) {
	cfg := &MemTableConfig{MaxSize: 100, MaxCount: 0, IdleDurationNanos: 0}
	m := NewMemTable(cfg)

	p := &types.Point{
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Tags:        map[string]string{"host": "server1"},
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}

	for !m.ShouldFlush() {
		if err := m.Write(p, 0); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if !m.ShouldFlush() {
		t.Error("expected ShouldFlush to return true")
	}
}

func TestMemTable_WriteOutOfOrder(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          1000,
		IdleDurationNanos: int64(time.Minute),
	}

	m := NewMemTable(cfg)

	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
		}
		if err := m.Write(p, 0); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 乱序写入
	p := &types.Point{
		Timestamp: 500000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(0.5)},
	}
	if err := m.Write(p, 0); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	count := 0
	iter := m.Iterator()
	for iter.Next() {
		count++
	}
	if count != 6 {
		t.Errorf("expected 6 entries, got %d", count)
	}
}

func TestMemTable_ShouldFlush_IdleTimeout(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          1000,
		IdleDurationNanos: int64(100 * time.Millisecond),
	}

	m := NewMemTable(cfg)

	p := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := m.Write(p, 0); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if m.ShouldFlush() {
		t.Error("ShouldFlush should return false immediately after write")
	}
}

func TestMemTable_FlushMultipleTimes(t *testing.T) {
	cfg := &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          1000,
		IdleDurationNanos: int64(time.Minute),
	}

	m := NewMemTable(cfg)

	for j := 0; j < 3; j++ {
		for i := 0; i < 5; i++ {
			p := &types.Point{
				Timestamp: int64(j*10+i) * 1e9,
				Tags:      map[string]string{"host": "server1"},
				Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(float64(i))},
			}
			if err := m.Write(p, 0); err != nil {
				t.Fatalf("Write failed: %v", err)
			}
		}

		points, sids := m.Flush()
		if len(points) != 5 || len(sids) != 5 {
			t.Errorf("expected 5 points in flush %d, got points=%d sids=%d", j, len(points), len(sids))
		}
	}

	if m.Count() != 0 {
		t.Errorf("expected 0 count after flush, got %d", m.Count())
	}
}

func TestMemTable_IdleTimeout(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	idleTimeout := m.IdleTimeout()
	if idleTimeout != time.Minute {
		t.Errorf("expected IdleTimeout %v, got %v", time.Minute, idleTimeout)
	}
}

func TestMemTable_IteratorEmpty(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	iter := m.Iterator()
	if iter.Next() {
		t.Error("empty iterator should return false")
	}
}
