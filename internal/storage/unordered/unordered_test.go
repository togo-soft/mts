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

	paths, err := Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("expected 1 path, got %d", len(paths))
	}
	if paths[0] == "" {
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
	reader, err := sstable.NewReader(paths[0], sstable.Schema{})
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

func TestWrite_MultipleDB(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	globalSeq.Store(0)

	mp1 := types.PointToMemPoint(&types.Point{
		Database:    "db1",
		Measurement: "meas1",
		Timestamp:   100,
		Fields:      map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))},
	}, 1)
	mp2 := types.PointToMemPoint(&types.Point{
		Database:    "db2",
		Measurement: "meas2",
		Timestamp:   200,
		Fields:      map[string]*types.FieldValue{"x": types.NewFieldValue(int64(42))},
	}, 2)

	paths, err := Write(dir, []types.MemPoint{mp1, mp2}, sstable.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("expected 2 paths, got %d", len(paths))
	}

	files, err := ListFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	// Verify ParseFilePath works
	for _, path := range paths {
		db, meas, _, ok := ParseFilePath(dir, path)
		if !ok {
			t.Errorf("failed to parse path %s", path)
		}
		if db != "db1" && db != "db2" {
			t.Errorf("unexpected db: %s", db)
		}
		if meas != "meas1" && meas != "meas2" {
			t.Errorf("unexpected meas: %s", meas)
		}
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

	paths, err := Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if err := Remove(paths[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paths[0]); !os.IsNotExist(err) {
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
	path := FilePath("/data", "db1", "meas1", 42)
	expected := filepath.Join("/data", "unordered", "db1", "meas1", "sst_42.bin")
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

func TestParseFilePath(t *testing.T) {
	dataDir := "/data"
	path := filepath.Join("/data", "unordered", "db1", "meas1", "sst_42.bin")
	db, meas, seq, ok := ParseFilePath(dataDir, path)
	if !ok {
		t.Fatal("expected ok")
	}
	if db != "db1" {
		t.Errorf("expected db1, got %s", db)
	}
	if meas != "meas1" {
		t.Errorf("expected meas1, got %s", meas)
	}
	if seq != 42 {
		t.Errorf("expected 42, got %d", seq)
	}
}

func TestParseFilePath_Invalid(t *testing.T) {
	dataDir := "/data"
	_, _, _, ok := ParseFilePath(dataDir, "/data/unordered/sst_1.bin")
	if ok {
		t.Error("expected false for flat file")
	}
}
