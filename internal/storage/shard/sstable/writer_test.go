package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"micro-ts/types"
)

func TestWriter_WritePoints(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": 85.5, "count": int64(100)},
		},
		{
			Timestamp: 2000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": 90.0, "count": int64(200)},
		},
	}

	err = w.WritePoints(points)
	if err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证 timestamp 文件存在
	tsPath := filepath.Join(tmpDir, "data", "sst_0", "_timestamps.bin")
	info, err := os.Stat(tsPath)
	if err != nil {
		t.Fatalf("stat timestamp file failed: %v", err)
	}
	if info.Size() == 0 {
		t.Errorf("timestamp file should not be empty")
	}

	// 验证 field 文件存在
	for _, name := range []string{"usage", "count"} {
		fieldPath := filepath.Join(tmpDir, "data", "sst_0", "fields", name+".bin")
		info, err := os.Stat(fieldPath)
		if err != nil {
			t.Fatalf("stat field %s file failed: %v", name, err)
		}
		if info.Size() == 0 {
			t.Errorf("field %s file should not be empty", name)
		}
	}
}
