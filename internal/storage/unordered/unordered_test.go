package unordered

import (
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

func TestEnsureDir(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	if err := EnsureDir(dataDir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(Dir(dataDir))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}
}

func TestNextSeq(t *testing.T) {
	globalSeq.Store(0)
	s1 := NextSeq()
	s2 := NextSeq()
	if s1 != 1 {
		t.Errorf("expected 1, got %d", s1)
	}
	if s2 != 2 {
		t.Errorf("expected 2, got %d", s2)
	}
}

func TestSetSeq(t *testing.T) {
	globalSeq.Store(0)
	SetSeq(100)
	if NextSeq() != 101 {
		t.Error("expected 101 after SetSeq(100)")
	}
}

func TestListFiles_Empty(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	files, err := ListFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
}

func TestWriteAndList(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	globalSeq.Store(0)

	mp := types.PointToMemPoint(&types.Point{
		Database:    "db1",
		Measurement: "meas1",
		Timestamp:   100,
		Fields:      map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))},
	}, 1)

	path, err := Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Fatal("expected non-empty path")
	}

	files, err := ListFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	// Verify it can be read and shows FlagUnordered
	reader, err := sstable.NewReader(path, sstable.Schema{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if reader.Flags != sstable.FlagUnordered {
		t.Errorf("expected FlagUnordered, got 0x%04x", reader.Flags)
	}

	rows, err := reader.ReadAll(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestRemove(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	globalSeq.Store(0)

	mp := types.PointToMemPoint(&types.Point{
		Database:    "db1",
		Measurement: "meas1",
		Timestamp:   100,
		Fields:      map[string]*types.FieldValue{},
	}, 1)

	path, err := Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if err := Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should have been removed")
	}
}

func TestRecoverSeq(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	globalSeq.Store(0)

	mp := types.PointToMemPoint(&types.Point{
		Database:    "db1",
		Measurement: "meas1",
		Timestamp:   100,
		Fields:      map[string]*types.FieldValue{},
	}, 1)

	_, _ = Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)
	_, _ = Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)

	globalSeq.Store(0)
	if err := RecoverSeq(dir); err != nil {
		t.Fatal(err)
	}
	if NextSeq() != 3 {
		t.Errorf("expected seq 3 after recovery, got %d", NextSeq())
	}
}

func TestWrite_EmptyPoints(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	globalSeq.Store(0)

	_, err := Write(dir, nil, sstable.CompressionNone)
	if err == nil {
		t.Error("expected error for nil points")
	}
	_, err = Write(dir, []types.MemPoint{}, sstable.CompressionNone)
	if err == nil {
		t.Error("expected error for empty points")
	}
}

func TestFilePath(t *testing.T) {
	path := FilePath("/data", 42)
	expected := filepath.Join("/data", "unordered", "sst_42.bin")
	if path != expected {
		t.Errorf("expected %s, got %s", expected, path)
	}
}

func TestListFiles_NonExistentDir(t *testing.T) {
	files, err := ListFiles("/nonexistent/path")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files for non-existent dir, got %d", len(files))
	}
}
