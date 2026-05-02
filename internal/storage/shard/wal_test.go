// internal/storage/shard/wal_test.go
package shard

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestWAL_Write(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	data := []byte("test data")
	n, err := w.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}
}

func TestWAL_Sync(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	data := []byte("test data")
	_, err = w.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err = w.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestWAL_Reopen(t *testing.T) {
	tmpDir := t.TempDir()
	w1, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	_, err = w1.Write([]byte("data1"))
	if err != nil {
		_ = w1.Close()
		t.Fatalf("Write failed: %v", err)
	}

	if err := w1.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	w2, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w2.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	if w2.Sequence() != 1 {
		t.Errorf("expected sequence 1, got %d", w2.Sequence())
	}
}

func TestWAL_FilePermissions(t *testing.T) {
	// Permission check skipped on Windows due to ACL differences
	if runtime.GOOS == "windows" {
		t.Skip("skipping permission test on Windows")
	}

	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	for _, f := range files {
		info, _ := os.Stat(f)
		if info.Mode().Perm() != 0600 {
			t.Errorf("expected 0600, got %o", info.Mode().Perm())
		}
	}
}
