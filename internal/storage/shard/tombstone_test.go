package shard

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
)

func TestTombstoneSet_ShouldDelete(t *testing.T) {
	ts := &compaction.TombstoneSet{
		Tombstones: []compaction.Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200, DeletedAt: 300},
			{SID: 2, MinTime: 150, MaxTime: 250, DeletedAt: 300},
		},
	}

	tests := []struct {
		name      string
		sid       uint64
		timestamp int64
		want      bool
	}{
		{"within first tombstone", 1, 150, true},
		{"at first tombstone start", 1, 100, true},
		{"at first tombstone end", 1, 200, true},
		{"before first tombstone", 1, 50, false},
		{"after first tombstone", 1, 250, false},
		{"within second tombstone", 2, 200, true},
		{"different sid in range", 3, 150, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ts.ShouldDelete(tt.sid, tt.timestamp)
			if got != tt.want {
				t.Errorf("ShouldDelete(%d, %d) = %v, want %v", tt.sid, tt.timestamp, got, tt.want)
			}
		})
	}
}

func TestTombstoneSet_Empty(t *testing.T) {
	ts := &compaction.TombstoneSet{}
	if ts.ShouldDelete(1, 100) {
		t.Error("empty TombstoneSet.ShouldDelete should return false")
	}
}

func TestTombstoneSet_HasTombstones(t *testing.T) {
	tests := []struct {
		name string
		ts   *compaction.TombstoneSet
		want bool
	}{
		{"nil", nil, false},
		{"empty", &compaction.TombstoneSet{}, false},
		{"with tombstones", &compaction.TombstoneSet{Tombstones: []compaction.Tombstone{{SID: 1}}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ts.HasTombstones(); got != tt.want {
				t.Errorf("HasTombstones() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadTombstones_NotExist(t *testing.T) {
	tmpDir := t.TempDir()
	ts, err := compaction.LoadTombstones(filepath.Join(tmpDir, "nonexistent"))
	if err != nil {
		t.Fatalf("loadTombstones should not error for nonexistent path: %v", err)
	}
	if ts != nil {
		t.Error("loadTombstones should return nil for nonexistent path")
	}
}

func TestLoadTombstones_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	partPath := filepath.Join(tmpDir, "part1")
	if err := os.MkdirAll(partPath, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	if err := os.WriteFile(tombstonePath, []byte("invalid json"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	ts, err := compaction.LoadTombstones(partPath)
	if err == nil {
		t.Error("loadTombstones should error for invalid JSON")
	}
	if ts != nil {
		t.Error("loadTombstones should return nil on error")
	}
}

func TestLoadTombstones_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	partPath := filepath.Join(tmpDir, "part1")
	if err := os.MkdirAll(partPath, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Write valid tombstone file directly with JSON
	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	data := []byte(`{"Tombstones":[{"sid":1,"mint":100,"maxt":200,"deleted":300}]}`)
	if err := os.WriteFile(tombstonePath, data, 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	loaded, err := compaction.LoadTombstones(partPath)
	if err != nil {
		t.Fatalf("loadTombstones failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("loadTombstones returned nil")
	}
	if len(loaded.Tombstones) != 1 {
		t.Errorf("expected 1 tombstone, got %d", len(loaded.Tombstones))
	}
	if loaded.Tombstones[0].SID != 1 {
		t.Errorf("expected SID 1, got %d", loaded.Tombstones[0].SID)
	}
}

func TestSaveTombstones_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	ts := &compaction.TombstoneSet{}
	err := compaction.SaveTombstones(tmpDir, ts)
	if err != nil {
		t.Fatalf("saveTombstones should not error for empty set: %v", err)
	}
}

func TestSaveTombstones_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	ts := &compaction.TombstoneSet{
		Tombstones: []compaction.Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200, DeletedAt: 300},
		},
	}

	partPath := filepath.Join(tmpDir, "part1")
	err := compaction.SaveTombstones(partPath, ts)
	if err != nil {
		t.Fatalf("saveTombstones failed: %v", err)
	}

	// Verify file exists
	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	if _, err := os.Stat(tombstonePath); os.IsNotExist(err) {
		t.Error("tombstone file should exist after save")
	}

	// Verify we can load it back
	loaded, err := compaction.LoadTombstones(partPath)
	if err != nil {
		t.Fatalf("loadTombstones failed: %v", err)
	}
	if len(loaded.Tombstones) != 1 {
		t.Errorf("expected 1 tombstone, got %d", len(loaded.Tombstones))
	}
}

func TestRemoveTombstones(t *testing.T) {
	tmpDir := t.TempDir()
	partPath := filepath.Join(tmpDir, "part1")
	if err := os.MkdirAll(partPath, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Create tombstone file
	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	if err := os.WriteFile(tombstonePath, []byte("{}"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Remove it
	if err := compaction.RemoveTombstones(partPath); err != nil {
		t.Fatalf("removeTombstones failed: %v", err)
	}

	// Verify it's gone
	if _, err := os.Stat(tombstonePath); !os.IsNotExist(err) {
		t.Error("tombstone file should not exist after remove")
	}
}

func TestRemoveTombstones_NotExist(t *testing.T) {
	tmpDir := t.TempDir()
	// Should not error when file doesn't exist
	err := compaction.RemoveTombstones(filepath.Join(tmpDir, "nonexistent"))
	if err != nil {
		t.Fatalf("removeTombstones should not error for nonexistent: %v", err)
	}
}

func TestLevelCompactionManager_CompactTombstones(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := compaction.DefaultLevelCompactionConfig()
	cfg.TombstoneRetention = 1 * time.Hour

	// Create a mock shard
	shard := &Shard{
		dir: tmpDir,
	}

	lcm, err := compaction.NewLevelCompactionManager(shard, cfg)
	if err != nil {
		t.Fatalf("NewLevelCompactionManager failed: %v", err)
	}
	defer lcm.Stop()

	// Create L0 directory with a part
	l0Path := lcm.Manifest.GetLevelPath(0)
	if err := os.MkdirAll(l0Path, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	partName := "part1"
	partPath := filepath.Join(l0Path, partName)
	if err := os.MkdirAll(partPath, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Create tombstone with old timestamp (expired)
	oldTime := time.Now().Add(-2 * time.Hour).Unix()
	ts := &compaction.TombstoneSet{
		Tombstones: []compaction.Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200, DeletedAt: oldTime},
		},
	}
	if err := compaction.SaveTombstones(partPath, ts); err != nil {
		t.Fatalf("saveTombstones failed: %v", err)
	}

	// Add part to manifest with DeletedAt=0 so it gets processed
	lcm.Manifest.AddPart(0, compaction.PartInfo{
		Name:      partName,
		DeletedAt: 0, // Must be 0 for CompactTombstones to process it
	})

	// Run compaction
	err = lcm.CompactTombstones()
	if err != nil {
		t.Fatalf("CompactTombstones failed: %v", err)
	}

	// Verify tombstone file was removed (all expired)
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

	// Create tombstone with mixed timestamps (some expired, some not)
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
		DeletedAt: 0, // Must be 0 for CompactTombstones to process it
	})

	err = lcm.CompactTombstones()
	if err != nil {
		t.Fatalf("CompactTombstones failed: %v", err)
	}

	// Verify tombstone file still exists with only active tombstone
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
