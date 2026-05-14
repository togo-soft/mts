// internal/storage/shard/sstable/iterator_test.go
package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func TestIterator_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建 writer 并关闭（不写入任何数据）
	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), Schema{Fields: make(map[string]FieldType)})
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	it, err := r.NewIterator(nil)
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

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
		},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), Schema{Fields: make(map[string]FieldType)})
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	it, err := r.NewIterator(nil)
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

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
		},
		{
			Timestamp: 2000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0)},
		},
		{
			Timestamp: 3000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(95.5)},
		},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), Schema{Fields: make(map[string]FieldType)})
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	it, err := r.NewIterator(nil)
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

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
		},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), Schema{Fields: make(map[string]FieldType)})
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Logf("Close failed: %v", err)
		}
	}()

	it, err := r.NewIterator(nil)
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

func TestIterator_ProjectedFields(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWriter(tmpDir, 1, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"cpu":  types.NewFieldValue(float64(1.5)),
				"mem":  types.NewFieldValue(float64(60.0)),
				"disk": types.NewFieldValue(float64(30.0)),
			},
		},
		{
			Timestamp: 2_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"cpu":  types.NewFieldValue(float64(2.0)),
				"mem":  types.NewFieldValue(float64(65.0)),
				"disk": types.NewFieldValue(float64(35.0)),
			},
		},
	}
	if err := w.WritePoints(pointsToInternalWithSids(points, nil)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := fmt.Sprintf("%s/data/sst_1.bin", tmpDir)
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator([]string{"cpu"})
	if err != nil {
		t.Fatalf("NewIterator with fields failed: %v", err)
	}

	if !it.Next() {
		t.Fatal("expected first row")
	}
	row := it.Point()
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if row.GetFieldValue("cpu") == nil {
		t.Error("expected cpu field")
	}
	if row.GetFieldValue("mem") != nil {
		t.Error("mem should not be present with field projection")
	}
	if row.GetFieldValue("disk") != nil {
		t.Error("disk should not be present with field projection")
	}
	if row.GetFieldValue("cpu").GetFloatValue() != float64(1.5) {
		t.Errorf("expected cpu=1.5, got %v", row.GetFieldValue("cpu"))
	}

	if !it.Next() {
		t.Fatal("expected second row")
	}
	row2 := it.Point()
	if row2.GetFieldValue("cpu").GetFloatValue() != float64(2.0) {
		t.Errorf("expected cpu=2.0, got %v", row2.GetFieldValue("cpu"))
	}
}

func TestIterator_AllFieldsNil(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWriter(tmpDir, 1, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"cpu": types.NewFieldValue(float64(1.0)),
				"mem": types.NewFieldValue(float64(60.0)),
			},
		},
	}
	if err := w.WritePoints(pointsToInternalWithSids(points, nil)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := fmt.Sprintf("%s/data/sst_1.bin", tmpDir)
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator(nil) failed: %v", err)
	}

	if !it.Next() {
		t.Fatal("expected row")
	}
	row := it.Point()
	if len(row.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(row.Fields))
	}
}
