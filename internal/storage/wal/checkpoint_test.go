package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpoint_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	cp := &Checkpoint{Generation: 42, Segment: 7}
	if err := cp.Save(dir); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCheckpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil checkpoint")
	}
	if loaded.Generation != 42 || loaded.Segment != 7 {
		t.Errorf("expected gen=42 seg=7, got gen=%d seg=%d", loaded.Generation, loaded.Segment)
	}
}

func TestCheckpoint_LoadNotExist(t *testing.T) {
	dir := t.TempDir()
	cp, err := LoadCheckpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cp != nil {
		t.Error("expected nil for nonexistent checkpoint")
	}
}

func TestCheckpoint_ClearCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cp := &Checkpoint{Generation: 1, Segment: 1}
	if err := cp.Save(dir); err != nil {
		t.Fatal(err)
	}
	if err := ClearCheckpoint(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(CheckpointPath(dir)); !os.IsNotExist(err) {
		t.Error("checkpoint file should be removed")
	}
}

func TestCheckpoint_ClearNonExistent(t *testing.T) {
	dir := t.TempDir()
	if err := ClearCheckpoint(dir); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpoint_SaveOverwrite(t *testing.T) {
	dir := t.TempDir()
	cp1 := &Checkpoint{Generation: 1, Segment: 3}
	cp2 := &Checkpoint{Generation: 2, Segment: 5}
	_ = cp1.Save(dir)
	_ = cp2.Save(dir)
	loaded, _ := LoadCheckpoint(dir)
	if loaded.Generation != 2 || loaded.Segment != 5 {
		t.Errorf("expected overwritten gen=2 seg=5, got gen=%d seg=%d", loaded.Generation, loaded.Segment)
	}
}

func TestCheckpointPath(t *testing.T) {
	p := CheckpointPath("/data/wal")
	expected := filepath.Join("/data/wal", checkpointFileName)
	if p != expected {
		t.Errorf("expected %s, got %s", expected, p)
	}
}
