package engine

import (
	"testing"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
)

func TestFlushCoordinator_New(t *testing.T) {
	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	fc := NewFlushCoordinator(mt, nil, nil, nil, "/tmp", sstable.CompressionNone)
	if fc == nil {
		t.Fatal("expected non-nil FlushCoordinator")
	}
	if fc.MemTable() != mt {
		t.Error("MemTable() should return the memtable")
	}
}

func TestFlushCoordinator_FlushAll_Empty(t *testing.T) {
	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	fc := NewFlushCoordinator(mt, nil, nil, nil, t.TempDir(), sstable.CompressionNone)
	if err := fc.FlushAll(); err != nil {
		t.Errorf("FlushAll on empty MemTable should not error: %v", err)
	}
}

func TestFlushCoordinator_Close(t *testing.T) {
	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	fc := NewFlushCoordinator(mt, nil, nil, nil, t.TempDir(), sstable.CompressionNone)
	fc.Close()
	// Second close should be safe (sync.Once)
	fc.Close()
}
