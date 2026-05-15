package engine

import (
	"sync"
	"testing"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// mockFlusher 实现 Flusher 接口用于测试。
type mockFlusher struct {
	mu       sync.Mutex
	flushed  map[string][]types.MemPoint
	flushErr error
}

func (f *mockFlusher) Flush(db, measurement string, points []types.MemPoint) error {
	if f.flushErr != nil {
		return f.flushErr
	}
	f.mu.Lock()
	key := db + "/" + measurement
	if f.flushed == nil {
		f.flushed = make(map[string][]types.MemPoint)
	}
	f.flushed[key] = append(f.flushed[key], points...)
	f.mu.Unlock()
	return nil
}

func (f *mockFlusher) Compact(db, measurement string, startTime int64) error { return nil }

func (f *mockFlusher) GetShards(db, measurement string, startTime, endTime int64) []*shard.Shard {
	return nil
}

func (f *mockFlusher) CloseAll() error                     { return nil }
func (f *mockFlusher) SetConfig(config *compaction.Config) {}

// mockWriter 实现 Writer 接口用于测试。
type mockWriter struct {
	mt          *memtable.MemTable
	seriesStore SeriesStore
	closed      bool
}

func (w *mockWriter) Write(point *types.Point) error {
	return w.mt.Write(types.PointToMemPoint(point, 0))
}

func (w *mockWriter) WriteBatch(points []*types.Point) (int, error) { return 0, nil }
func (w *mockWriter) MemTable() *memtable.MemTable                  { return w.mt }
func (w *mockWriter) SeriesStore() SeriesStore                      { return w.seriesStore }
func (w *mockWriter) Close() error                                  { w.closed = true; return nil }

func TestFlushCoordinator_RegisterAndGet(t *testing.T) {
	t.Parallel()

	fc := NewFlushCoordinator(&mockFlusher{})

	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	w := &mockWriter{mt: mt}

	fc.RegisterWriter("db1", "cpu", w)

	got := fc.GetWriter("db1", "cpu")
	if got == nil {
		t.Fatal("expected writer, got nil")
	}

	notFound := fc.GetWriter("db2", "cpu")
	if notFound != nil {
		t.Fatal("expected nil for unregistered writer")
	}
}

func TestFlushCoordinator_FlushWriter_Empty(t *testing.T) {
	t.Parallel()

	fc := NewFlushCoordinator(&mockFlusher{})

	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	w := &mockWriter{mt: mt}

	fc.RegisterWriter("db1", "cpu", w)

	err := fc.FlushWriter("db1", "cpu")
	if err != nil {
		t.Fatalf("FlushWriter with empty memtable should succeed: %v", err)
	}
}

func TestFlushCoordinator_FlushWriter_WithData(t *testing.T) {
	t.Parallel()

	flusher := &mockFlusher{}
	fc := NewFlushCoordinator(flusher)

	cfg := memtable.DefaultMemTableConfig()
	cfg.MaxSize = 1024
	mt := memtable.NewMemTable(cfg)
	w := &mockWriter{mt: mt}

	// 写入测试数据到 MemTable
	mp := types.MemPoint{Timestamp: 100, Sid: 1}
	_ = mt.Write(mp)
	_ = mt.Write(types.MemPoint{Timestamp: 200, Sid: 2})

	fc.RegisterWriter("db1", "cpu", w)

	err := fc.FlushWriter("db1", "cpu")
	if err != nil {
		t.Fatalf("FlushWriter failed: %v", err)
	}

	flusher.mu.Lock()
	key := "db1/cpu"
	if _, ok := flusher.flushed[key]; !ok {
		t.Fatal("expected data to be flushed")
	}
	if len(flusher.flushed[key]) != 2 {
		t.Fatalf("expected 2 points flushed, got %d", len(flusher.flushed[key]))
	}
	flusher.mu.Unlock()
}

func TestFlushCoordinator_CloseAllWriters(t *testing.T) {
	t.Parallel()

	fc := NewFlushCoordinator(&mockFlusher{})

	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	w := &mockWriter{mt: mt}

	fc.RegisterWriter("db1", "cpu", w)

	err := fc.CloseAllWriters()
	if err != nil {
		t.Fatalf("CloseAllWriters failed: %v", err)
	}
}

func TestFlushCoordinator_FlushWriter_Unregistered(t *testing.T) {
	t.Parallel()

	fc := NewFlushCoordinator(&mockFlusher{})
	err := fc.FlushWriter("db_not_exists", "cpu")
	if err != nil {
		t.Fatalf("FlushWriter for unregistered writer should return nil: %v", err)
	}
}

func TestFlushCoordinator_FlushWriter_FlushError(t *testing.T) {
	t.Parallel()

	flusher := &mockFlusher{flushErr: assertAnError}
	fc := NewFlushCoordinator(flusher)

	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	w := &mockWriter{mt: mt}

	_ = mt.Write(types.MemPoint{Timestamp: 100, Sid: 1})
	fc.RegisterWriter("db1", "cpu", w)

	err := fc.FlushWriter("db1", "cpu")
	if err == nil {
		t.Fatal("expected error from FlushWriter when flusher fails")
	}
}

// assertAnError 供测试占位使用。
var assertAnError = &errFlushFailed{}

type errFlushFailed struct{}

func (e *errFlushFailed) Error() string { return "flush failed" }
