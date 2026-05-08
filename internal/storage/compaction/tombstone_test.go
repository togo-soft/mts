package compaction

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTombstoneSet_ShouldDelete(t *testing.T) {
	ts := &TombstoneSet{
		Tombstones: []Tombstone{
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
	ts := &TombstoneSet{}
	if ts.ShouldDelete(1, 100) {
		t.Error("empty TombstoneSet.ShouldDelete should return false")
	}
}

func TestTombstoneSet_HasTombstones(t *testing.T) {
	tests := []struct {
		name string
		ts   *TombstoneSet
		want bool
	}{
		{"nil", nil, false},
		{"empty", &TombstoneSet{}, false},
		{"with tombstones", &TombstoneSet{Tombstones: []Tombstone{{SID: 1}}}, true},
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
	ts, err := LoadTombstones(filepath.Join(tmpDir, "nonexistent"))
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

	ts, err := LoadTombstones(partPath)
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

	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	data := []byte(`{"Tombstones":[{"sid":1,"mint":100,"maxt":200,"deleted":300}]}`)
	if err := os.WriteFile(tombstonePath, data, 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	loaded, err := LoadTombstones(partPath)
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
	ts := &TombstoneSet{}
	err := SaveTombstones(tmpDir, ts)
	if err != nil {
		t.Fatalf("saveTombstones should not error for empty set: %v", err)
	}
}

func TestSaveTombstones_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	ts := &TombstoneSet{
		Tombstones: []Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200, DeletedAt: 300},
		},
	}

	partPath := filepath.Join(tmpDir, "part1")
	err := SaveTombstones(partPath, ts)
	if err != nil {
		t.Fatalf("saveTombstones failed: %v", err)
	}

	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	if _, err := os.Stat(tombstonePath); os.IsNotExist(err) {
		t.Error("tombstone file should exist after save")
	}

	loaded, err := LoadTombstones(partPath)
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

	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	if err := os.WriteFile(tombstonePath, []byte("{}"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := RemoveTombstones(partPath); err != nil {
		t.Fatalf("removeTombstones failed: %v", err)
	}

	if _, err := os.Stat(tombstonePath); !os.IsNotExist(err) {
		t.Error("tombstone file should not exist after remove")
	}
}

func TestRemoveTombstones_NotExist(t *testing.T) {
	tmpDir := t.TempDir()
	err := RemoveTombstones(filepath.Join(tmpDir, "nonexistent"))
	if err != nil {
		t.Fatalf("removeTombstones should not error for nonexistent: %v", err)
	}
}
