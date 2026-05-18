package engine

import (
	"testing"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/types"
)

func TestFlushCoordinator_New(t *testing.T) {
	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	fc := NewFlushCoordinator(mt, nil, nil, nil, "/tmp", types.CompressionNone)
	if fc == nil {
		t.Fatal("expected non-nil FlushCoordinator")
	}
	if fc.MemTable() != mt {
		t.Error("MemTable() should return the memtable")
	}
}

func TestFlushCoordinator_FlushAll_Empty(t *testing.T) {
	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	fc := NewFlushCoordinator(mt, nil, nil, nil, t.TempDir(), types.CompressionNone)
	if err := fc.FlushAll(); err != nil {
		t.Errorf("FlushAll on empty MemTable should not error: %v", err)
	}
}

func TestFlushCoordinator_Close(t *testing.T) {
	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	fc := NewFlushCoordinator(mt, nil, nil, nil, t.TempDir(), types.CompressionNone)
	fc.Close()
	// Second close should be safe (sync.Once)
	fc.Close()
}
