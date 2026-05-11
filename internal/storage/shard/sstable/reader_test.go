package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func TestReader_ReadAll(t *testing.T) {
	tmpDir := t.TempDir()

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

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := filepath.Join(tmpDir, "data", "sst_0.bin")
	r, err := NewReader(sstPath, schema)
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
	if rows[0].Timestamp != 1000 {
		t.Errorf("expected timestamp 1000, got %d", rows[0].Timestamp)
	}
}

func TestReader_ReadTimestamps(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)}},
		{Timestamp: 2000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0)}},
		{Timestamp: 3000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(95.5)}},
	}
	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证单文件 SSTable 存在
	sstPath := filepath.Join(tmpDir, "data", "sst_0.bin")
	info, err := os.Stat(sstPath)
	if err != nil {
		t.Fatalf("stat sst file failed: %v", err)
	}
	if info.Size() == 0 {
		t.Errorf("sst file should not be empty")
	}

	// 通过 Reader 验证数据
	r, err := NewReader(sstPath, schema)
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
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

func TestReader_ReadRange_NoIndex(t *testing.T) {
	tmpDir := t.TempDir()

	// blockSize=0 表示无索引（全扫描模式）
	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)}},
		{Timestamp: 2000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0)}},
		{Timestamp: 3000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(95.5)}},
	}
	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := filepath.Join(tmpDir, "data", "sst_0.bin")
	r, err := NewReader(sstPath, schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	rows, err := r.ReadRange(1500, 2500)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("expected 1 row in range [1500, 2500), got %d", len(rows))
	}
}

func TestReader_ReadRange_AllFields(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{
			"float_val": types.NewFieldValue(3.14),
			"int_val":   types.NewFieldValue(int64(42)),
			"str_val":   types.NewFieldValue("hello"),
			"bool_val":  types.NewFieldValue(true),
		}},
	}
	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := filepath.Join(tmpDir, "data", "sst_0.bin")
	r, err := NewReader(sstPath, schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	rows, err := r.ReadRange(0, 2000)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	row := rows[0]
	if row.Fields["float_val"] == nil {
		t.Error("float_val should be present")
	}
	if row.Fields["int_val"] == nil {
		t.Error("int_val should be present")
	}
	if row.Fields["str_val"] == nil {
		t.Error("str_val should be present")
	}
	if row.Fields["bool_val"] == nil {
		t.Error("bool_val should be present")
	}
}

func TestReader_ReadSids_NotExist(t *testing.T) {
	// 测试读取不存在的 SSTable 文件
	tmpDir := t.TempDir()

	sstPath := filepath.Join(tmpDir, "data", "sst_nonexistent.bin")
	_, err := NewReader(sstPath, Schema{Fields: make(map[string]FieldType)})
	if err == nil {
		t.Error("expected error for nonexistent sst file")
	}
}

func TestReader_ReadTimestamps_NotExist(t *testing.T) {
	// 测试读取不存在的 SSTable 文件
	tmpDir := t.TempDir()

	sstPath := filepath.Join(tmpDir, "data", "sst_0.bin")
	_, err := NewReader(sstPath, Schema{Fields: make(map[string]FieldType)})
	if err == nil {
		t.Error("expected error for nonexistent timestamps file")
	}
}
