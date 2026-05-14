package compaction

import (
	"encoding/json"
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
	ts, err := LoadTombstones(filepath.Join(tmpDir, "sst_0.bin"))
	if err != nil {
		t.Fatalf("loadTombstones should not error for nonexistent path: %v", err)
	}
	if ts != nil {
		t.Error("loadTombstones should return nil for nonexistent path")
	}
}

func TestLoadTombstones_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	partPath := filepath.Join(tmpDir, "sst_0.bin")

	tombstonePath := partPath + ".tombstones"
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
	partPath := filepath.Join(tmpDir, "sst_0.bin")

	tombstonePath := partPath + ".tombstones"
	data := []byte(`{"tombstones":[{"sid":1,"mint":100,"maxt":200,"deleted":300}]}`)
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
	partPath := filepath.Join(tmpDir, "sst_0.bin")
	ts := &TombstoneSet{}
	err := SaveTombstones(partPath, ts)
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

	partPath := filepath.Join(tmpDir, "sst_0.bin")
	err := SaveTombstones(partPath, ts)
	if err != nil {
		t.Fatalf("saveTombstones failed: %v", err)
	}

	tombstonePath := partPath + ".tombstones"
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
	partPath := filepath.Join(tmpDir, "sst_0.bin")

	tombstonePath := partPath + ".tombstones"
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
	err := RemoveTombstones(filepath.Join(tmpDir, "sst_0.bin"))
	if err != nil {
		t.Fatalf("removeTombstones should not error for nonexistent: %v", err)
	}
}

func TestTombstoneSet_BuildIndex(t *testing.T) {
	ts := &TombstoneSet{
		Tombstones: []Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200, DeletedAt: 300},
			{SID: 1, MinTime: 500, MaxTime: 600, DeletedAt: 300},
			{SID: 2, MinTime: 150, MaxTime: 250, DeletedAt: 300},
			{SID: 3, MinTime: 0, MaxTime: 1000, DeletedAt: 300},
		},
	}
	ts.BuildIndex()

	tests := []struct {
		name      string
		sid       uint64
		timestamp int64
		want      bool
	}{
		{"match first of same SID", 1, 100, true},
		{"match second of same SID", 1, 550, true},
		{"gap between same SID ranges", 1, 350, false},
		{"different SID match", 2, 200, true},
		{"SID with single range", 3, 500, true},
		{"SID not in index", 99, 100, false},
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

func TestTombstoneSet_BuildIndex_Empty(t *testing.T) {
	ts := &TombstoneSet{}
	ts.BuildIndex()
	if ts.ShouldDelete(1, 100) {
		t.Error("empty indexed TombstoneSet.ShouldDelete should return false")
	}
}

func TestCollectInputTombstones(t *testing.T) {
	dir := t.TempDir()

	// 创建带 tombstones 的 SSTable 文件
	sstPath1 := filepath.Join(dir, "sst_1.bin")
	_ = os.WriteFile(sstPath1, []byte("sst data"), 0600)
	tsData1, _ := json.Marshal(&TombstoneSet{
		Tombstones: []Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200},
			{SID: 2, MinTime: 300, MaxTime: 400},
		},
	})
	_ = os.WriteFile(sstPath1+".tombstones", tsData1, 0600)

	// SSTable 不带 tombstones
	sstPath2 := filepath.Join(dir, "sst_2.bin")
	_ = os.WriteFile(sstPath2, []byte("sst data"), 0600)

	// 不存在的 SSTable 文件（跳过）
	sstPath3 := filepath.Join(dir, "sst_3.bin")

	paths := []string{sstPath1, sstPath2, sstPath3}
	result := collectInputTombstones(paths)

	if result == nil {
		t.Fatal("expected non-nil TombstoneSet")
	}
	if len(result.Tombstones) != 2 {
		t.Errorf("expected 2 tombstones from file 1, got %d", len(result.Tombstones))
	}
}

func TestCollectInputTombstones_EmptyInput(t *testing.T) {
	result := collectInputTombstones(nil)
	if result == nil {
		t.Fatal("expected non-nil empty TombstoneSet")
	}
	if len(result.Tombstones) != 0 {
		t.Errorf("expected 0 tombstones, got %d", len(result.Tombstones))
	}
}
