// internal/storage/shard/sstable/iterator_test.go
package sstable

import (
	"path/filepath"
	"testing"

	"micro-ts/internal/types"
)

func TestIterator_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建 writer 并关闭（不写入任何数据）
	w, err := NewWriter(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 验证空表返回 false
	if it.Next() {
		t.Errorf("expected false for empty SSTable, got true")
	}

	// 验证 Point 返回 nil
	if pt := it.Point(); pt != nil {
		t.Errorf("expected nil point for empty SSTable, got %+v", pt)
	}
}

func TestIterator_SingleRecord(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": 85.5},
		},
	}

	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 验证可以读取一条数据
	if !it.Next() {
		t.Errorf("expected true for single record")
	}

	pt := it.Point()
	if pt == nil {
		t.Fatalf("expected point, got nil")
	}
	if pt.Timestamp != 1000 {
		t.Errorf("expected timestamp 1000, got %d", pt.Timestamp)
	}

	// 验证没有更多数据
	if it.Next() {
		t.Errorf("expected false after last record")
	}
}

func TestIterator_MultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": 85.5},
		},
		{
			Timestamp: 2000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": 90.0},
		},
		{
			Timestamp: 3000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": 95.5},
		},
	}

	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 验证时间有序
	timestamps := []int64{1000, 2000, 3000}
	for i, expected := range timestamps {
		if !it.Next() {
			t.Fatalf("expected true at index %d", i)
		}
		pt := it.Point()
		if pt == nil {
			t.Fatalf("expected point at index %d, got nil", i)
		}
		if pt.Timestamp != expected {
			t.Errorf("expected timestamp %d at index %d, got %d", expected, i, pt.Timestamp)
		}
	}

	// 验证没有更多数据
	if it.Next() {
		t.Errorf("expected false after all records")
	}
}

func TestIterator_NextBeyondRange(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"usage": 85.5},
		},
	}

	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 第一次 Next
	if !it.Next() {
		t.Errorf("expected true")
	}

	// 第二次 Next（超出范围）
	if it.Next() {
		t.Errorf("expected false when beyond range")
	}

	// 第三次 Next（仍然超出范围）
	if it.Next() {
		t.Errorf("expected false when beyond range")
	}
}
