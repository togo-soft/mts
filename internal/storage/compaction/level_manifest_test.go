package compaction

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLevelSpec_DefaultLevelSpecs(t *testing.T) {
	configs := DefaultLevelSpecs()
	if len(configs) != 5 {
		t.Errorf("expected 5 level configs, got %d", len(configs))
	}

	if configs[0].Level != 0 || configs[0].MaxParts != 10 {
		t.Errorf("L0 config incorrect: %+v", configs[0])
	}

	if configs[1].Level != 1 || configs[1].MaxSize != 100*1024*1024 {
		t.Errorf("L1 config incorrect: %+v", configs[1])
	}
}

func TestLevelManifest_NewLevelManifest(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, err := NewLevelManifest(dataDir, nil)
	if err != nil {
		t.Fatalf("NewLevelManifest failed: %v", err)
	}

	if manifest == nil {
		t.Fatal("manifest should not be nil")
	}

	for i := 0; i < 5; i++ {
		l := manifest.GetLevel(i)
		if l == nil {
			t.Errorf("level %d should exist", i)
			continue
		}
		if l.Level != i {
			t.Errorf("level %d has incorrect Level field", i)
		}
	}

	for i := 0; i < 5; i++ {
		levelPath := manifest.GetLevelPath(i)
		if _, err := os.Stat(levelPath); os.IsNotExist(err) {
			t.Errorf("level directory L%d should exist at %s", i, levelPath)
		}
	}
}

func TestLevelManifest_NextSeq(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := NewLevelManifest(dataDir, nil)

	seq1 := manifest.NextSeq()
	seq2 := manifest.NextSeq()
	seq3 := manifest.NextSeq()

	if seq1 == seq2 || seq2 == seq3 || seq1 == seq3 {
		t.Error("NextSeq should return unique values")
	}
	if seq2 != seq1+1 || seq3 != seq2+1 {
		t.Error("NextSeq should increment by 1")
	}
}

func TestLevelManifest_SetNextSeq(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := NewLevelManifest(dataDir, nil)

	seq1 := manifest.NextSeq()
	if seq1 != 0 {
		t.Errorf("initial seq should be 0, got %d", seq1)
	}

	manifest.SetNextSeq(100)
	seq2 := manifest.NextSeq()
	if seq2 != 100 {
		t.Errorf("after SetNextSeq(100), NextSeq should return 100, got %d", seq2)
	}
	seq3 := manifest.NextSeq()
	if seq3 != 101 {
		t.Errorf("after SetNextSeq(100), NextSeq should increment to 101, got %d", seq3)
	}
}

func TestLevelManifest_AddRemovePart(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := NewLevelManifest(dataDir, nil)

	part := PartInfo{
		Name:    "sst_00000000000000000001",
		Size:    1024,
		MinTime: 1000,
		MaxTime: 2000,
	}

	manifest.AddPart(0, part)

	l0 := manifest.GetLevel(0)
	if len(l0.Parts) != 1 {
		t.Errorf("expected 1 part, got %d", len(l0.Parts))
	}
	if l0.Size != 1024 {
		t.Errorf("expected size 1024, got %d", l0.Size)
	}

	manifest.RemovePart(0, "sst_00000000000000000001")
	if len(l0.Parts) != 0 {
		t.Errorf("expected 0 parts, got %d", len(l0.Parts))
	}
	if l0.Size != 0 {
		t.Errorf("expected size 0, got %d", l0.Size)
	}
}

func TestLevelManifest_RemoveParts(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := NewLevelManifest(dataDir, nil)

	manifest.AddPart(0, PartInfo{Name: "sst_1", Size: 100, MinTime: 1000, MaxTime: 2000})
	manifest.AddPart(0, PartInfo{Name: "sst_2", Size: 200, MinTime: 2000, MaxTime: 3000})
	manifest.AddPart(0, PartInfo{Name: "sst_3", Size: 300, MinTime: 3000, MaxTime: 4000})

	manifest.RemoveParts(0, []string{"sst_1", "sst_3"})

	l0 := manifest.GetLevel(0)
	if len(l0.Parts) != 1 {
		t.Errorf("expected 1 remaining part, got %d", len(l0.Parts))
	}
	if l0.Parts[0].Name != "sst_2" {
		t.Errorf("expected sst_2 to remain, got %s", l0.Parts[0].Name)
	}
	if l0.Size != 200 {
		t.Errorf("expected size 200, got %d", l0.Size)
	}
}

func TestLevelManifest_SaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest1, _ := NewLevelManifest(dataDir, nil)

	manifest1.AddPart(0, PartInfo{Name: "sst_00000000000000000001", Size: 100, MinTime: 1000, MaxTime: 2000})
	manifest1.AddPart(0, PartInfo{Name: "sst_00000000000000000002", Size: 200, MinTime: 2000, MaxTime: 3000})
	manifest1.AddPart(1, PartInfo{Name: "sst_00000000000000000100", Size: 5000, MinTime: 1000, MaxTime: 5000})

	if err := manifest1.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	manifest2, _ := NewLevelManifest(dataDir, nil)
	if err := manifest2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	l0 := manifest2.GetLevel(0)
	if len(l0.Parts) != 2 {
		t.Errorf("expected 2 parts in L0, got %d", len(l0.Parts))
	}

	l1 := manifest2.GetLevel(1)
	if len(l1.Parts) != 1 {
		t.Errorf("expected 1 part in L1, got %d", len(l1.Parts))
	}

	if l1.Size != 5000 {
		t.Errorf("expected L1 size 5000, got %d", l1.Size)
	}
}

func TestLevelManifest_SaveLoad_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest1, _ := NewLevelManifest(dataDir, nil)
	manifest1.SetNextSeq(42)

	if err := manifest1.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	manifest2, _ := NewLevelManifest(dataDir, nil)
	if err := manifest2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// nextSeq should be preserved
	seq := manifest2.NextSeq()
	if seq != 42 {
		t.Errorf("expected nextSeq=42 after load, got %d", seq)
	}
}

func TestLevelManifest_Load_NotExist(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := NewLevelManifest(dataDir, nil)

	// Load should succeed silently when manifest file doesn't exist
	err := manifest.Load()
	if err != nil {
		t.Fatalf("Load should not error for nonexistent file: %v", err)
	}
}

func TestLevelConfig_DefaultLevelConfig(t *testing.T) {
	cfg := DefaultLevelConfig()
	if cfg == nil {
		t.Fatal("DefaultLevelSpec should not return nil")
	}
	if !cfg.Enabled {
		t.Error("Enabled should be true by default")
	}
	if cfg.CheckInterval != 5*time.Minute {
		t.Errorf("CheckInterval should be 5 minutes, got %v", cfg.CheckInterval)
	}
	if cfg.Timeout != 30*time.Minute {
		t.Errorf("Timeout should be 30 minutes, got %v", cfg.Timeout)
	}
}

func TestCheckpoint_SaveClear(t *testing.T) {
	tmpDir := t.TempDir()

	cp := &Checkpoint{
		Version:    1,
		Level:      0,
		OutputSeq:  42,
		OutputPath: "/tmp/sst_42",
		StartedAt:  time.Now().Unix(),
	}

	if err := cp.Save(tmpDir); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists
	path := cp.CheckpointPath(tmpDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("checkpoint file should exist after save")
	}

	// Load into a new checkpoint
	cp2 := &Checkpoint{}
	if err := cp2.Load(tmpDir); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cp2.Version != 1 {
		t.Errorf("expected Version=1, got %d", cp2.Version)
	}
	if cp2.Level != 0 {
		t.Errorf("expected Level=0, got %d", cp2.Level)
	}
	if cp2.OutputSeq != 42 {
		t.Errorf("expected OutputSeq=42, got %d", cp2.OutputSeq)
	}

	// Clear
	if err := cp.Clear(tmpDir); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("checkpoint file should not exist after clear")
	}
}

func TestCheckpoint_Clear_NotExist(t *testing.T) {
	tmpDir := t.TempDir()
	cp := &Checkpoint{}
	err := cp.Clear(tmpDir)
	if err != nil {
		t.Fatalf("Clear should not error for nonexistent file: %v", err)
	}
}

func TestPartInfo_HasOverlap(t *testing.T) {
	tests := []struct {
		name string
		p1   PartInfo
		p2   PartInfo
		want bool
	}{
		{
			"overlapping",
			PartInfo{MinTime: 100, MaxTime: 200},
			PartInfo{MinTime: 150, MaxTime: 250},
			true,
		},
		{
			"touching at boundary",
			PartInfo{MinTime: 100, MaxTime: 200},
			PartInfo{MinTime: 200, MaxTime: 300},
			true,
		},
		{
			"no overlap",
			PartInfo{MinTime: 100, MaxTime: 200},
			PartInfo{MinTime: 300, MaxTime: 400},
			false,
		},
		{
			"p1 contains p2",
			PartInfo{MinTime: 100, MaxTime: 500},
			PartInfo{MinTime: 200, MaxTime: 300},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasOverlap(tt.p1, tt.p2); got != tt.want {
				t.Errorf("HasOverlap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLevelSpec_Fields(t *testing.T) {
	lc := LevelSpec{Level: -1, MaxSize: 1024, MaxParts: 10}
	data, err := json.Marshal(lc)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	var lc2 LevelSpec
	if err := json.Unmarshal(data, &lc2); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if lc2.Level != lc.Level || lc2.MaxSize != lc.MaxSize || lc2.MaxParts != lc.MaxParts {
		t.Error("LevelSpec round-trip mismatch")
	}
}
