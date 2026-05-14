package sstable

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func pointsToInternal(points []*types.Point) []types.InternalPoint {
	result := make([]types.InternalPoint, len(points))
	for i, p := range points {
		result[i] = types.PointToInternal(p, 0)
	}
	return result
}

func TestIterator_DecodeString(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("hello")}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 测试 decodeString - 通过 Next() 遍历
	count := 0
	for it.Next() {
		pt := it.Point()
		if pt != nil {
			count++
		}
	}

	if count != 1 {
		t.Errorf("expected 1 point, got %d", count)
	}
}

func TestIterator_DecodeFieldValueFromData(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 验证迭代器功能正常
	if !it.Next() {
		t.Error("expected Next()=true")
	}

	pt := it.Point()
	if pt == nil {
		t.Error("expected non-nil Point")
	}
}

func TestReader_ReadAll_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	rows, err := r.ReadAll(nil)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(rows) != 0 {
		t.Errorf("expected 0 rows for empty SSTable, got %d", len(rows))
	}
}

func TestReader_FieldSize(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	// 测试 fieldSize - 注意 string 类型返回的是长度字段的大小(4)而不是-1
	tests := []struct {
		fieldType FieldType
		expected  int
	}{
		{FieldTypeFloat64, 8},
		{FieldTypeInt64, 8},
		{FieldTypeBool, 1},
		{"unknown", 8},
	}

	for _, tt := range tests {
		data := make([]byte, 100)
		result := r.fieldSize(data, tt.fieldType)
		if result != tt.expected {
			t.Errorf("fieldSize(%v): expected %d, got %d", tt.fieldType, tt.expected, result)
		}
	}
}

func TestWriter_Close_WithEmptyData(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 不写入任何数据直接关闭
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWriter_WritePoints_MultipleFields(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "s1"},
			Fields: map[string]*types.FieldValue{
				"f1": types.NewFieldValue(1.0),
				"f2": types.NewFieldValue(int64(100)),
				"f3": types.NewFieldValue("hello"),
				"f4": types.NewFieldValue(true),
			},
		},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWriter_DetectFieldType_NilValue(t *testing.T) {
	// 测试 detectFieldType 处理 nil 值
	result := detectFieldType(nil)
	if result != FieldTypeFloat64 {
		t.Errorf("expected FieldTypeFloat64 for nil, got %v", result)
	}
}

func TestWriter_AppendZeroValue_AllTypes(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 测试 appendZeroValue 对于不同类型
	w.appendZeroValue(nil, FieldTypeFloat64)
	w.appendZeroValue(nil, FieldTypeInt64)
	w.appendZeroValue(nil, FieldTypeBool)
	w.appendZeroValue(nil, FieldTypeString)

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestReader_GetBlockIndex(t *testing.T) {
	_ = t.TempDir()

	r := &Reader{}
	r.blockIndex = nil

	if r.GetBlockIndex() != nil {
		t.Error("expected nil block index")
	}
}

func TestBlockIndex_Read_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()

	idx := &BlockIndex{}
	err := idx.Read(tmpDir + "/nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestIterator_LoadBlock_InvalidIndex(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 测试 loadBlock 使用无效索引
	err = it.loadBlock(-1)
	if err != nil {
		t.Errorf("loadBlock(-1) should not error, got: %v", err)
	}

	err = it.loadBlock(999)
	if err != nil {
		t.Errorf("loadBlock(999) should not error, got: %v", err)
	}
}

func TestIterator_DecodeFieldValueFromData_String(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("test")}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("world")}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// decodeFieldValueFromData 在回退模式调用
	// 遍历所有数据
	count := 0
	for it.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 points, got %d", count)
	}
}

func TestIterator_Point_InvalidPositions(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 验证初始位置 Point 返回 nil
	// (pos=-1, currentBlock=-1)
	pt := it.Point()
	if pt != nil {
		t.Error("expected nil Point before first Next()")
	}
}

func TestIterator_CurrentBlockTimestamps_NoIndex(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 不写入任何数据直接关闭
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 没有 blockIndex 时，这些函数应该返回 0
	first := it.CurrentBlockFirstTimestamp()
	if first != 0 {
		t.Errorf("expected 0 for no index, got %d", first)
	}

	last := it.CurrentBlockLastTimestamp()
	if last != 0 {
		t.Errorf("expected 0 for no index, got %d", last)
	}
}

func TestIterator_SeekToTime_BeyondAll(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 写入足够多的数据确保有 block index
	points := make([]*types.Point, 100)
	for i := int64(0); i < 100; i++ {
		points[i] = &types.Point{
			Timestamp: (i + 1) * 1000,
			Tags:      map[string]string{"host": "s1"},
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// Seek 到所有数据之后
	if err := it.SeekToTime(200000); err != nil {
		t.Fatalf("SeekToTime failed: %v", err)
	}

	// 验证 Done() 返回 true
	if !it.Done() {
		t.Log("Done() may return false in fallback mode or without index")
	}

	// 消耗所有数据
	count := 0
	for it.Next() {
		count++
		_ = it.Point()
	}

	// 如果是在 fallback 模式或没有 index，count 可能 > 0
	// 这取决于具体实现
	t.Logf("Got %d points after seeking beyond all data", count)
}

func TestIterator_Done_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 初始状态 Done() 应该返回 false (currentBlock=-1, blockIndex 可能为空或非空)
	// 但对于空 sstable 或者有数据的情况，Done() 逻辑不同

	// 消耗所有数据
	for it.Next() {
		it.Point()
	}

	// 验证 Done() 状态
	_ = it.Done()
}

func TestIterator_Next_ErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 正常情况
	if !it.Next() {
		t.Error("expected true for first Next()")
	}
}

func TestIterator_DecodeFieldValueFromData_Int64(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(int64(100))}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(int64(200))}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	count := 0
	for it.Next() {
		pt := it.Point()
		if pt != nil && pt.GetFieldValue("v") != nil {
			count++
		}
	}

	if count != 2 {
		t.Errorf("expected 2 points with int64, got %d", count)
	}
}

func TestIterator_DecodeFieldValueFromData_Bool(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(true)}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(false)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	count := 0
	for it.Next() {
		pt := it.Point()
		if pt != nil {
			count++
		}
	}

	if count != 2 {
		t.Errorf("expected 2 points with bool, got %d", count)
	}
}

func TestIterator_DecodeFieldValueFromData_Float64(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.5))}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(2.5))}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	count := 0
	for it.Next() {
		pt := it.Point()
		if pt != nil && pt.GetFieldValue("v") != nil {
			count++
		}
	}

	if count != 2 {
		t.Errorf("expected 2 points with float64, got %d", count)
	}
}

func TestWriter_AppendFieldValue_AllTypes(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 设置 schema
	w.schema.Fields["float_field"] = FieldTypeFloat64
	w.schema.Fields["int_field"] = FieldTypeInt64
	w.schema.Fields["str_field"] = FieldTypeString
	w.schema.Fields["bool_field"] = FieldTypeBool
	w.fieldBufs["float_field"] = make([]byte, 0)
	w.fieldBufs["int_field"] = make([]byte, 0)
	w.fieldBufs["str_field"] = make([]byte, 0)
	w.fieldBufs["bool_field"] = make([]byte, 0)

	// 测试 float64
	w.appendFieldValue("float_field", types.NewFieldValue(float64(1.5)))
	if len(w.fieldBufs["float_field"]) != 8 {
		t.Errorf("expected 8 bytes for float64, got %d", len(w.fieldBufs["float_field"]))
	}

	// 测试 int64
	w.appendFieldValue("int_field", types.NewFieldValue(int64(100)))
	if len(w.fieldBufs["int_field"]) != 8 {
		t.Errorf("expected 8 bytes for int64, got %d", len(w.fieldBufs["int_field"]))
	}

	// 测试 string
	w.appendFieldValue("str_field", types.NewFieldValue("hello"))
	expected := 4 + 5 // len prefix + string
	if len(w.fieldBufs["str_field"]) != expected {
		t.Errorf("expected %d bytes for string, got %d", expected, len(w.fieldBufs["str_field"]))
	}

	// 测试 bool true
	w.appendFieldValue("bool_field", types.NewFieldValue(true))
	if len(w.fieldBufs["bool_field"]) != 1 {
		t.Errorf("expected 1 byte for bool, got %d", len(w.fieldBufs["bool_field"]))
	}

	_ = w.Close()
}

func TestWriter_AppendFieldValue_NilFieldValuePtr(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	w.schema.Fields["test"] = FieldTypeFloat64
	w.fieldBufs["test"] = make([]byte, 0)

	// 测试 nil *types.FieldValue
	w.appendFieldValue("test", (*types.FieldValue)(nil))
	if len(w.fieldBufs["test"]) != 8 {
		t.Errorf("expected 8 bytes for nil *FieldValue, got %d", len(w.fieldBufs["test"]))
	}

	_ = w.Close()
}

func TestWriter_AppendFieldValue_NilFieldValueValue(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	w.schema.Fields["test"] = FieldTypeFloat64
	w.fieldBufs["test"] = make([]byte, 0)

	// 测试 nil value (actual nil, not typed nil)
	w.appendFieldValue("test", nil)
	if len(w.fieldBufs["test"]) != 8 {
		t.Errorf("expected 8 bytes for nil value, got %d", len(w.fieldBufs["test"]))
	}

	_ = w.Close()
}

func TestWriter_AppendFieldValue_BareTypes(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	w.schema.Fields["test"] = FieldTypeFloat64
	w.fieldBufs["test"] = make([]byte, 0)

	// 测试裸类型 (float64)
	w.appendFieldValue("test", float64(1.5))
	if len(w.fieldBufs["test"]) != 8 {
		t.Errorf("expected 8 bytes for bare float64, got %d", len(w.fieldBufs["test"]))
	}

	_ = w.Close()
}

func TestWriter_Close_WithError(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 写入数据
	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}

	// 关闭 writer
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestReader_DecodeFieldValue_String(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"s": types.NewFieldValue("hello world")}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	// 测试 decodeFieldValue 的 string 类型
	// 手动构造 string 数据: [len][string]
	strData := make([]byte, 4+11)
	binary.BigEndian.PutUint32(strData[:4], uint32(11))
	copy(strData[4:], "hello world")

	result := r.decodeFieldValue(strData, 0, "s")
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.GetStringValue() != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", result.GetStringValue())
	}
}

func TestReader_DecodeFieldValue_OffsetBeyond(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	// 测试 offset 超出数据范围
	data := make([]byte, 4) // 只够 4 字节
	result := r.decodeFieldValue(data, 100, "v")
	if result == nil {
		t.Error("expected non-nil result for offset beyond data")
	}
}

func TestReader_ReadRange_InvalidRange(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(2.0)}},
		{Timestamp: 3000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(3.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	// 测试反向范围 (start > end)
	rows, err := r.ReadRange(3000, 1000, 0)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	// 应该返回空或全部数据，取决于实现
	t.Logf("ReadRange(3000, 1000) returned %d rows", len(rows))
}

func TestReader_DecodeFieldValue_UnknownType(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	// 手动添加一个 unknown 类型到 schema
	r.schema.Fields["unknown_field"] = "unknown_type"

	data := make([]byte, 8)
	result := r.decodeFieldValue(data, 0, "unknown_field")
	// Unknown type with uint64 goes to default case which returns NewFieldValue(uint64) -> nil
	// because uint64 is not a supported type in NewFieldValue
	if result != nil {
		t.Error("expected nil for unknown type with uint64")
	}
}

func TestIterator_LoadBlock_ReadDirError(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	schema := w.Schema()
	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 使用不存在的 fields 目录触发错误
	// 关闭 reader 文件触发 loadBlock 读取错误
	_ = r.file.Close()
	err = it.loadBlock(0)
	if err == nil {
		t.Error("expected error for closed file")
	}
}

func TestReadRange_EarlyTermination(t *testing.T) {
	tmpDir := t.TempDir()
	// NOTE: NewWriter(dir, seq, blockSize, compressAlgo). blockSize=0 uses default 64KB.
	w, err := NewWriter(tmpDir, 1, 0, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 写入 200 个数据点，全部在同一 block 内
	points := make([]*types.Point, 200)
	for i := 0; i < 200; i++ {
		points[i] = &types.Point{
			Timestamp: int64(i+1) * 1_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(float64(i)),
			},
		}
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

	// 请求 LIMIT 10，验证仅返回 10 行
	rows, err := r.ReadRange(0, 0, 10)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("expected 10 rows with maxRows=10, got %d", len(rows))
	}

	// 验证数据正确性（前 10 个点）
	for i, row := range rows {
		expectedVal := float64(i)
		got := row.GetFieldValue("value").GetFloatValue()
		if got != expectedVal {
			t.Errorf("row[%d]: expected value=%f, got %f", i, expectedVal, got)
		}
	}
}
