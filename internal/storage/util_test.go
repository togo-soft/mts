// internal/storage/util_test.go
package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSafeMkdirAll(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test", "nested", "dir")

	err := safeMkdirAll(testPath, 0700)
	if err != nil {
		t.Fatalf("safeMkdirAll failed: %v", err)
	}

	info, err := os.Stat(testPath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("expected directory")
	}
}

func TestSafeCreate(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	f, err := safeCreate(testFile, 0600)
	if err != nil {
		t.Fatalf("safeCreate failed: %v", err)
	}
	_ = f.Close()

	info, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("expected 0600, got %o", info.Mode().Perm())
	}
}

func TestSafeOpenFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	f, err := safeOpenFile(testFile, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("safeOpenFile failed: %v", err)
	}
	_ = f.Close()

	info, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("expected 0600, got %o", info.Mode().Perm())
	}
}
