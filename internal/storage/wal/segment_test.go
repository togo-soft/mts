package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSegment_CreatesFile(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegment(tmpDir, 0x1234, 1)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	expectedName := "0000000000001234_00000001.wal"
	expectedPath := filepath.Join(tmpDir, expectedName)
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("expected file %s to exist", expectedName)
	}

	if !seg.headerWritten {
		t.Error("expected header to be written")
	}
	if seg.size != segmentHeaderSize {
		t.Errorf("expected size %d, got %d", segmentHeaderSize, seg.size)
	}
}

func TestOpenSegment_ReopensExisting(t *testing.T) {
	tmpDir := t.TempDir()

	seg1, err := openSegment(tmpDir, 0xABCD, 2)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	if err := seg1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	seg2, err := openSegment(tmpDir, 0xABCD, 2)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer func() { _ = seg2.Close() }()

	if !seg2.headerWritten {
		t.Error("expected header to be already written")
	}
}

func TestSegment_Write(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegment(tmpDir, 1, 1)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	data := []byte("test data")
	n, err := seg.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}
	if seg.size != segmentHeaderSize+int64(len(data)) {
		t.Errorf("expected size %d, got %d", segmentHeaderSize+len(data), seg.size)
	}
}

func TestSegment_Truncate(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := openSegment(tmpDir, 1, 1)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	_, _ = seg.Write([]byte("some data"))
	if err := seg.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if seg.size != segmentHeaderSize {
		t.Errorf("expected size %d after truncate, got %d", segmentHeaderSize, seg.size)
	}
}

func TestListSegments(t *testing.T) {
	tmpDir := t.TempDir()

	for i := uint64(1); i <= 3; i++ {
		seg, err := openSegment(tmpDir, 1, i)
		if err != nil {
			t.Fatalf("openSegment %d: %v", i, err)
		}
		_ = seg.Close()
	}

	entries, err := listSegments(tmpDir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
	for i, e := range entries {
		expectedNum := uint64(i + 1)
		if e.Num != expectedNum {
			t.Errorf("entry %d: expected num %d, got %d", i, expectedNum, e.Num)
		}
	}
}

func TestListSegments_Sorted(t *testing.T) {
	tmpDir := t.TempDir()

	// Create segments out of order
	for _, num := range []uint64{3, 1, 2} {
		seg, err := openSegment(tmpDir, 1, num)
		if err != nil {
			t.Fatalf("openSegment %d: %v", num, err)
		}
		_ = seg.Close()
	}

	entries, err := listSegments(tmpDir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for i := uint64(1); i <= 3; i++ {
		if entries[i-1].Num != i {
			t.Errorf("entry %d: expected num %d, got %d", i-1, i, entries[i-1].Num)
		}
	}
}

func TestSegmentName(t *testing.T) {
	name := segmentName(0xABCD, 3)
	expected := "000000000000abcd_00000003.wal"
	if name != expected {
		t.Errorf("expected %q, got %q", expected, name)
	}
}

func TestParseSegmentName_Valid(t *testing.T) {
	gen, num, err := parseSegmentName("000000000000abcd_00000003.wal")
	if err != nil {
		t.Fatalf("parseSegmentName: %v", err)
	}
	if gen != 0xABCD {
		t.Errorf("expected gen 0xABCD, got %x", gen)
	}
	if num != 3 {
		t.Errorf("expected num 3, got %d", num)
	}
}

func TestParseSegmentName_Invalid(t *testing.T) {
	tests := []string{
		"notawalfile",
		"too_short.wal",
		"0000000000000001_00000002.txt",
		"gggggggggggggggg_00000001.wal",
	}
	for _, name := range tests {
		_, _, err := parseSegmentName(name)
		if err == nil {
			t.Errorf("parseSegmentName(%q) expected error", name)
		}
	}
}
