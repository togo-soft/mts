package engine

import (
	"sync"
	"testing"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// mockFlusher 实现 Flusher 接口用于测试。
type mockFlusher struct {
	mu       sync.Mutex
	flushed  map[string][]types.MemPoint
	flushErr error
}

func (f *mockFlusher) Flush(points []types.MemPoint) error {
	if f.flushErr != nil {
		return f.flushErr
	}
	f.mu.Lock()
	if f.flushed == nil {
		f.flushed = make(map[string][]types.MemPoint)
	}
	f.flushed["global"] = append(f.flushed["global"], points...)
	f.mu.Unlock()
	return nil
}

func (f *mockFlusher) Compact(startTime int64) error { return nil }

func (f *mockFlusher) GetShards(db, measurement string, startTime, endTime int64) []*shard.Shard {
	return nil
}

func (f *mockFlusher) CloseAll() error                     { return nil }
func (f *mockFlusher) SetConfig(config *compaction.Config) {}

func TestFlushCoordinator_FlushAll(t *testing.T) {
	t.Parallel()
	fc := NewFlushCoordinator(&mockFlusher{})
	if err := fc.FlushAll(); err != nil {
		t.Errorf("FlushAll should succeed: %v", err)
	}
}
