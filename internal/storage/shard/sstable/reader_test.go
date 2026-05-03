// internal/storage/shard/sstable/reader_test.go
package sstable

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReader_ReadTimestamps(t *testing.T) {
	tmpDir := t.TempDir()

	// 先创建 writer 并写入数据
	w, err := NewWriter(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	timestamps := []int64{1000, 2000, 3000}
	if err := w.WriteTimestampBlock(timestamps); err != nil {
		t.Fatalf("WriteTimestampBlock failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 使用 reader 读取
	r, err := NewReader(filepath.Join(tmpDir, "data", "_timestamps.bin"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	// 验证文件可以打开读取
	info, err := os.Stat(filepath.Join(tmpDir, "data", "_timestamps.bin"))
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Errorf("file should not be empty")
	}
}
