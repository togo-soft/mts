// internal/storage/shard/sstable/writer_test.go
package sstable

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriter_WriteTimestampBlock(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(filepath.Join(tmpDir, "timestamps.bin"))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	timestamps := []int64{1000, 2000, 3000}
	err = w.WriteTimestampBlock(timestamps)
	if err != nil {
		t.Fatalf("WriteTimestampBlock failed: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证文件存在
	info, err := os.Stat(filepath.Join(tmpDir, "timestamps.bin"))
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Errorf("file should not be empty")
	}
}
