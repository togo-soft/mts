package shard

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
)

func TestLevelCompactionManager_CompactTombstones(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := compaction.DefaultLevelCompactionConfig()
	cfg.TombstoneRetention = 1 * time.Hour

	shard := &Shard{
		dir: tmpDir,
	}

	lcm, err := compaction.NewLevelCompactionManager(shard, cfg)
	if err != nil {
		t.Fatalf("NewLevelCompactionManager failed: %v", err)
	}
	defer lcm.Stop()

	l0Path := lcm.Manifest.GetLevelPath(0)
	if err := os.MkdirAll(l0Path, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	partName := "part1"
	partPath := filepath.Join(l0Path, partName)
	if err := os.MkdirAll(partPath, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	ts := &compaction.TombstoneSet{
		Tombstones: []compaction.Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200, DeletedAt: oldTime},
		},
	}
	if err := compaction.SaveTombstones(partPath, ts); err != nil {
		t.Fatalf("saveTombstones failed: %v", err)
	}

	lcm.Manifest.AddPart(0, compaction.PartInfo{
		Name:      partName,
		DeletedAt: 0,
	})

	err = lcm.CompactTombstones()
	if err != nil {
		t.Fatalf("CompactTombstones failed: %v", err)
	}

	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	if _, statErr := os.Stat(tombstonePath); !os.IsNotExist(statErr) {
		t.Error("expired tombstone file should be removed")
	}
}

func TestLevelCompactionManager_CompactTombstones_PartialRetention(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := compaction.DefaultLevelCompactionConfig()
	cfg.TombstoneRetention = 1 * time.Hour

	shard := &Shard{
		dir: tmpDir,
	}

	lcm, err := compaction.NewLevelCompactionManager(shard, cfg)
	if err != nil {
		t.Fatalf("NewLevelCompactionManager failed: %v", err)
	}
	defer lcm.Stop()

	l0Path := lcm.Manifest.GetLevelPath(0)
	if err := os.MkdirAll(l0Path, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	partName := "part1"
	partPath := filepath.Join(l0Path, partName)
	if err := os.MkdirAll(partPath, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	newTime := time.Now().Unix()
	ts := &compaction.TombstoneSet{
		Tombstones: []compaction.Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200, DeletedAt: oldTime}, // expired
			{SID: 2, MinTime: 300, MaxTime: 400, DeletedAt: newTime}, // active
		},
	}
	if err := compaction.SaveTombstones(partPath, ts); err != nil {
		t.Fatalf("saveTombstones failed: %v", err)
	}

	lcm.Manifest.AddPart(0, compaction.PartInfo{
		Name:      partName,
		DeletedAt: 0,
	})

	err = lcm.CompactTombstones()
	if err != nil {
		t.Fatalf("CompactTombstones failed: %v", err)
	}

	loaded, err := compaction.LoadTombstones(partPath)
	if err != nil {
		t.Fatalf("loadTombstones failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("tombstone file should exist")
	}
	if len(loaded.Tombstones) != 1 {
		t.Errorf("expected 1 active tombstone, got %d", len(loaded.Tombstones))
	}
	if loaded.Tombstones[0].SID != 2 {
		t.Errorf("expected SID 2, got %d", loaded.Tombstones[0].SID)
	}
}
