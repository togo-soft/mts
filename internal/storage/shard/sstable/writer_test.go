package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func pointsToInternalWithSids(points []*types.Point, sids []uint64) []types.InternalPoint {
	result := make([]types.InternalPoint, len(points))
	for i, p := range points {
		sid := uint64(0)
		if i < len(sids) {
			sid = sids[i]
		}
		result[i] = types.PointToInternal(p, sid)
	}
	return result
}

func TestWriter_WritePoints(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
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

	err = w.WritePoints(pointsToInternalWithSids(points, nil))
	if err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}

	err = w.Close()
	if err != nil {
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
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	rows, err := r.ReadAll(nil)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

func TestWriter_WritePointsWithSids(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(int64(1))},
		},
		{
			Timestamp: 2000,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(int64(2))},
		},
	}
	sids := []uint64{42, 99}

	if err := w.WritePoints(pointsToInternalWithSids(points, sids)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证单文件 SSTable 存在
	sstPath := filepath.Join(tmpDir, "data", "sst_1.bin")
	info, err := os.Stat(sstPath)
	if err != nil {
		t.Fatalf("stat sst file failed: %v", err)
	}
	if info.Size() < 64 {
		t.Errorf("sst file too small, got %d bytes", info.Size())
	}

	// 通过 Reader 验证 SID
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	rows, err := r.ReadAll(nil)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Sid != 42 {
		t.Errorf("expected first row Sid=42, got %d", rows[0].Sid)
	}
	if rows[1].Sid != 99 {
		t.Errorf("expected second row Sid=99, got %d", rows[1].Sid)
	}
}

func TestWriter_DictEncodingRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// 低基数字符串：字典编码应有收益
	points := make([]types.InternalPoint, 200)
	statuses := []string{"ok", "error", "timeout", "ok", "ok", "error"}
	for i := range points {
		points[i] = types.InternalPoint{
			Timestamp: int64((i + 1) * 100),
			Sid:       uint64(i % 10),
			Fields: []types.InternalField{
				{Key: "status", Value: types.NewFieldValue(statuses[i%len(statuses)])},
			},
		}
	}

	w, err := NewWriter(dir, 0, 512, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 验证 schema 正确记录
	if ft, ok := schema.Fields["status"]; !ok || ft != FieldTypeString {
		t.Fatalf("expected string field 'status', got %v", schema.Fields)
	}

	// 读取验证
	r, err := NewReader(filepath.Join(dir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}

	var count int
	for it.Next() {
		row := it.Point()
		if row.Fields["status"] == nil {
			t.Errorf("row %d missing status field", count)
		}
		count++
	}
	if count != 200 {
		t.Errorf("expected 200 rows, got %d", count)
	}
}

func TestWriter_DictEncodingLargeDataset(t *testing.T) {
	dir := t.TempDir()

	// 10000 行低基数字符串，验证 streaming 不 OOM
	points := make([]types.InternalPoint, 10000)
	values := []string{"a", "b", "c", "d", "a", "b"}
	for i := range points {
		points[i] = types.InternalPoint{
			Timestamp: int64((i + 1) * 100),
			Sid:       uint64(i % 50),
			Fields: []types.InternalField{
				{Key: "label", Value: types.NewFieldValue(values[i%len(values)])},
			},
		}
	}

	w, err := NewWriter(dir, 0, 64*1024, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 完整读取验证行数
	r, err := NewReader(filepath.Join(dir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}

	count := 0
	for it.Next() {
		count++
	}
	if count != 10000 {
		t.Errorf("expected 10000 rows, got %d", count)
	}
}

func TestWriter_DictEncodingFallback(t *testing.T) {
	dir := t.TempDir()

	// 高基数随机字符串：字典编码应自动回退为 raw
	points := make([]types.InternalPoint, 100)
	for i := range points {
		points[i] = types.InternalPoint{
			Timestamp: int64((i + 1) * 100),
			Sid:       uint64(i),
			Fields: []types.InternalField{
				{Key: "uuid", Value: types.NewFieldValue(fmt.Sprintf("id-%d-%x", i, i*37))},
			},
		}
	}

	w, err := NewWriter(dir, 0, 64*1024, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(filepath.Join(dir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}

	count := 0
	for it.Next() {
		row := it.Point()
		if row.Fields["uuid"] == nil {
			t.Errorf("row %d missing uuid field", count)
		}
		count++
	}
	if count != 100 {
		t.Errorf("expected 100 rows, got %d", count)
	}
}
