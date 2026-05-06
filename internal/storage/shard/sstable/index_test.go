// internal/storage/shard/sstable/index_test.go
package sstable

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBlockIndex_WriteRead(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "_index.bin")

	idx := &BlockIndex{}

	// 添加 3 个 block entries
	idx.Add(1000, 1999, 0, 100)
	idx.Add(2000, 2999, 65536, 100)
	idx.Add(3000, 3999, 131072, 100)

	// 写入
	if err := idx.Write(indexFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 读取
	idx2 := &BlockIndex{}
	if err := idx2.Read(indexFile); err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// 验证
	if idx2.Len() != 3 {
		t.Errorf("expected 3 entries, got %d", idx2.Len())
	}

	if idx2.Entry(0).FirstTimestamp != 1000 {
		t.Errorf("expected first ts 1000, got %d", idx2.Entry(0).FirstTimestamp)
	}
}

func TestBlockIndex_FindBlock(t *testing.T) {
	idx := &BlockIndex{}

	idx.Add(1000, 1999, 0, 100)
	idx.Add(2000, 2999, 65536, 100)
	idx.Add(3000, 3999, 131072, 100)

	tests := []struct {
		target int64
		expect int
	}{
		{500, 0},  // 在第一个 block 之前
		{1000, 0}, // 第一个 block 的起始
		{1500, 0}, // 第一个 block 中间
		{2000, 1}, // 第二个 block 的起始
		{2500, 1}, // 第二个 block 中间
		{3000, 2}, // 第三个 block 的起始
		{5000, 3}, // 超过所有 block
	}

	for _, tc := range tests {
		result := idx.FindBlock(tc.target)
		if result != tc.expect {
			t.Errorf("FindBlock(%d) = %d, expect %d", tc.target, result, tc.expect)
		}
	}
}

func TestBlockIndex_ReadInvalidFile(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "_invalid_index.bin")

	// 创建空文件
	f, err := os.Create(indexFile)
	if err != nil {
		t.Fatalf("Create file failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	idx := &BlockIndex{}
	if err := idx.Read(indexFile); err == nil {
		t.Errorf("expected error for invalid index file, got nil")
	}
}

func TestBlockIndex_ReadCorruptFile(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "_corrupt_index.bin")

	// 创建损坏的文件 (错误 magic)
	f, err := os.Create(indexFile)
	if err != nil {
		t.Fatalf("Create file failed: %v", err)
	}
	if _, err := f.Write([]byte("CORRUPT")); err != nil {
		_ = f.Close()
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	idx := &BlockIndex{}
	if err := idx.Read(indexFile); err == nil {
		t.Errorf("expected error for corrupt index file, got nil")
	}
}
