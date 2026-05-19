package downsample

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseShardDir_Valid(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantStart int64
		wantEnd   int64
	}{
		{"simple", "100_200", 100, 200},
		{"large numbers", "1700000000000000000_1700000000100000000", 1700000000000000000, 1700000000100000000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok := parseShardDir(tt.input)
			if !ok {
				t.Fatal("parseShardDir returned false")
			}
			if start != tt.wantStart {
				t.Errorf("start = %d, want %d", start, tt.wantStart)
			}
			if end != tt.wantEnd {
				t.Errorf("end = %d, want %d", end, tt.wantEnd)
			}
		})
	}
}

func TestParseShardDir_Invalid(t *testing.T) {
	tests := []string{
		"",
		"no_underscore",
		"abc_200",
		"100_xyz",
		"100_200_300",
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, _, ok := parseShardDir(input)
			if ok {
				t.Errorf("expected false for input %q", input)
			}
		})
	}
}

func TestListSSTFiles_Empty(t *testing.T) {
	dir := t.TempDir()
	files, err := listSSTFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Error("expected 0 files in empty dir")
	}
}

func TestListSSTFiles_WithFiles(t *testing.T) {
	dir := t.TempDir()
	// Create some .bin files
	for _, name := range []string{"sst_1.bin", "sst_2.bin", "other.txt"} {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}

	files, err := listSSTFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}
}

func TestListSSTFiles_Nonexistent(t *testing.T) {
	dir := t.TempDir()
	nonexistent := filepath.Join(dir, "nonexistent")
	_, err := listSSTFiles(nonexistent)
	if err == nil {
		t.Error("expected error for nonexistent dir")
	}
}

func TestIsDownsampleDone(t *testing.T) {
	dir := t.TempDir()
	if isDownsampleDone(dir) {
		t.Error("should not be done for empty dir")
	}

	_ = markDownsampleDone(dir)
	if !isDownsampleDone(dir) {
		t.Error("should be done after marking")
	}
}

func TestMarkDownsampleDone(t *testing.T) {
	dir := t.TempDir()
	err := markDownsampleDone(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Verify the file exists
	_, err = os.Stat(filepath.Join(dir, "_downsample_done"))
	if err != nil {
		t.Fatal("_downsample_done file not found:", err)
	}
}

func TestNextSSTSeq_Empty(t *testing.T) {
	dir := t.TempDir()
	seq, err := nextSSTSeq(dir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Errorf("expected seq 1, got %d", seq)
	}
}

func TestNextSSTSeq_WithFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"sst_1.bin", "sst_3.bin", "sst_2.bin"} {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}

	seq, err := nextSSTSeq(dir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 4 {
		t.Errorf("expected seq 4, got %d", seq)
	}
}

func TestNextSSTSeq_Nonexistent(t *testing.T) {
	dir := t.TempDir()
	nonexistent := filepath.Join(dir, "nonexistent")
	seq, err := nextSSTSeq(nonexistent)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Errorf("expected seq 1 for nonexistent dir, got %d", seq)
	}
}

func TestNextSSTSeq_IgnoresOtherFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"sst_1.bin", "other.txt", "_downsample_done"} {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}

	seq, err := nextSSTSeq(dir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 2 {
		t.Errorf("expected seq 2, got %d", seq)
	}
}
