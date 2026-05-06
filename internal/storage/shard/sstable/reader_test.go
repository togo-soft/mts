// internal/storage/shard/sstable/reader_test.go
package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func TestReader_ReadAll(t *testing.T) {
	tmpDir := t.TempDir()

	// 先写入
	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5), "count": types.NewFieldValue(int64(100))},
		},
		{
			Timestamp: 2000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0), "count": types.NewFieldValue(int64(200))},
		},
	}

	if err := w.WritePoints(points, nil); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 再读取
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	rows, err := r.ReadAll(nil)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}

	// 验证第一条数据
	if rows[0].Timestamp != 1000 {
		t.Errorf("expected timestamp 1000, got %d", rows[0].Timestamp)
	}
}

func TestReader_ReadTimestamps(t *testing.T) {
	tmpDir := t.TempDir()

	// 先创建 writer 并写入数据
	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)}},
		{Timestamp: 2000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0)}},
		{Timestamp: 3000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(95.5)}},
	}
	if err := w.WritePoints(points, nil); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 使用 reader 读取
	r, err := NewReader(tmpDir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	// 验证数据文件存在
	info, err := os.Stat(filepath.Join(tmpDir, "data", "sst_0", "_timestamps.bin"))
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Errorf("file should not be empty")
	}
}
