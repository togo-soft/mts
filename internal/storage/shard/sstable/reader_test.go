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

func TestReader_ReadRange_NoIndex(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建 writer 并写入数据
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

	// 删除索引文件以触发 readRangeFullScan
	indexPath := filepath.Join(tmpDir, "data", "sst_0", "_index.bin")
	if err := os.Remove(indexPath); err != nil {
		t.Fatalf("Failed to remove index file: %v", err)
	}

	// 使用 reader 读取范围（应该触发 readRangeFullScan）
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		_ = r.Close()
	}()

	// ReadRange 应该回退到全表扫描
	rows, err := r.ReadRange(1500, 2500)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	// 应该只返回时间戳在范围内的行
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
	if err := w.WritePoints(points, nil); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		_ = r.Close()
	}()

	rows, err := r.ReadRange(0, 2000)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// 验证所有字段类型
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
	// 测试 sids 文件不存在的情况
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}
	if err := w.WritePoints(points, nil); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 删除 sids 文件
	sidPath := filepath.Join(tmpDir, "data", "sst_0", "_sids.bin")
	if err := os.Remove(sidPath); err != nil {
		t.Fatalf("Remove sids file failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		_ = r.Close()
	}()

	// ReadAll 应该能处理缺失的 sids 文件
	rows, err := r.ReadAll(nil)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestReader_ReadTimestamps_NotExist(t *testing.T) {
	// 测试 timestamps 文件不存在的情况
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}
	if err := w.WritePoints(points, nil); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 删除 timestamps 文件
	tsPath := filepath.Join(tmpDir, "data", "sst_0", "_timestamps.bin")
	if err := os.Remove(tsPath); err != nil {
		t.Fatalf("Remove timestamps file failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		_ = r.Close()
	}()

	// ReadAll 应该返回错误
	_, err = r.ReadAll(nil)
	if err == nil {
		t.Error("expected error for missing timestamps file")
	}
}
