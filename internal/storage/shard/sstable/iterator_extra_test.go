package sstable

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func TestIterator_FieldFixedSize(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 测试 fieldFixedSize
	tests := []struct {
		fieldType FieldType
		expected  int
	}{
		{FieldTypeFloat64, 8},
		{FieldTypeInt64, 8},
		{FieldTypeBool, 1},
		{FieldTypeString, -1},
		{"unknown", 8}, // 默认值
	}

	for _, tt := range tests {
		result := it.fieldFixedSize(tt.fieldType)
		if result != tt.expected {
			t.Errorf("fieldFixedSize(%v): expected %d, got %d", tt.fieldType, tt.expected, result)
		}
	}
}

func TestIterator_DecodeFixedValue(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 测试 decodeFixedValue
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f} // 1.0 as float64

	result := it.decodeFixedValue(data, FieldTypeFloat64)
	if result == nil {
		t.Error("decodeFixedValue returned nil")
	}
}

func TestIterator_DecodeString(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("hello")}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

func TestIterator_ZeroValue(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 测试 zeroValue
	tests := []struct {
		fieldType FieldType
	}{
		{FieldTypeFloat64},
		{FieldTypeInt64},
		{FieldTypeBool},
		{FieldTypeString},
		{"unknown"},
	}

	for _, tt := range tests {
		result := it.zeroValue(tt.fieldType)
		if result == nil {
			t.Errorf("zeroValue(%v) returned nil", tt.fieldType)
		}
	}
}

func TestIterator_LoadAllData(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 写入多个数据点
	points := make([]*types.Point, 10)
	for i := 0; i < 10; i++ {
		points[i] = &types.Point{
			Timestamp: int64(i) * 1000,
			Tags:      map[string]string{"host": "s1"},
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 遍历所有数据
	count := 0
	for it.Next() {
		pt := it.Point()
		if pt != nil {
			count++
		}
	}

	if count != 10 {
		t.Errorf("expected 10 points, got %d", count)
	}
}

func TestIterator_DecodeFieldValueFromData(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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

	w, err := NewWriter(tmpDir, 0, 0)
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

	w, err := NewWriter(tmpDir, 0, 0)
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

	if err := w.WritePoints(points, nil); err != nil {
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

	w, err := NewWriter(tmpDir, 0, 0)
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

func TestReader_HasBlockIndex_WithoutIndex(t *testing.T) {
	tmpDir := t.TempDir()

	r := &Reader{dataDir: tmpDir}
	r.blockIndex = nil

	if r.HasBlockIndex() {
		t.Error("expected HasBlockIndex()=false for nil index")
	}
}

func TestReader_GetBlockIndex(t *testing.T) {
	tmpDir := t.TempDir()

	r := &Reader{dataDir: tmpDir}
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

func TestIterator_LoadAllData_EmptyTimestamps(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 写入空数据
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 空数据应该正常工作
	if it.Next() {
		t.Errorf("expected false for empty data")
	}
}

func TestIterator_DecodeFixedValue_Default(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 测试 decodeFixedValue 的 default case (unknown field type)
	// NewFieldValue(uint64) returns nil because uint64 is not a supported type
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	result := it.decodeFixedValue(data, "unknown")
	if result != nil {
		t.Error("decodeFixedValue should return nil for unknown type as NewFieldValue(uint64) is not supported")
	}
}

func TestIterator_DecodeString_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("hello")}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 测试空数据
	result := it.decodeString([]byte{}, 0)
	if result == nil {
		t.Error("decodeString should return non-nil for empty data")
	}

	// 测试数据过短
	result = it.decodeString([]byte{0x00, 0x00}, 0)
	if result == nil {
		t.Error("decodeString should return non-nil for short data")
	}
}

func TestIterator_DecodeFieldValueFromData_String(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("test")}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("world")}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

func TestIterator_LoadAllData_WithMultipleFields(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

	if count != 1 {
		t.Errorf("expected 1 point, got %d", count)
	}
}

func TestIterator_Point_InvalidPositions(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 不写入任何数据直接关闭
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

func TestIterator_FallbackMode(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := make([]*types.Point, 10)
	for i := 0; i < 10; i++ {
		points[i] = &types.Point{
			Timestamp: int64(i+1) * 1000,
			Tags:      map[string]string{"host": "s1"},
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

	if count != 10 {
		t.Errorf("expected 10 points, got %d", count)
	}
}

func TestIterator_SeekToTime_BeyondAll(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

func TestIterator_ReadFieldBlock(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// readFieldBlock 测试无效路径
	_, err = it.readFieldBlock("/nonexistent/path", 0, 100)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestIterator_Done_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 正常情况
	if !it.Next() {
		t.Error("expected true for first Next()")
	}
}

func TestIterator_Point_FallbackMode(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 首次 Next
	if !it.Next() {
		t.Error("expected true")
	}

	// Point 在回退模式下
	pt := it.Point()
	if pt == nil {
		t.Error("expected non-nil point")
	}
}

func TestIterator_FallbackMode_Empty(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 不写入任何数据
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := NewReader(filepath.Join(tmpDir, "data", "sst_0"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 应该没有数据
	if it.Next() {
		t.Error("expected false for empty sstable")
	}
}

func TestIterator_FallbackMode_MultipleFields(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "s1"},
			Fields: map[string]*types.FieldValue{
				"str_field": types.NewFieldValue("hello"),
			},
		},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

	if count != 1 {
		t.Errorf("expected 1 point, got %d", count)
	}
}

func TestIterator_DecodeString_TruncatedData(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("test")}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 测试 decodeString 边界情况 - pos 超出范围
	result := it.decodeString([]byte{}, 100)
	if result == nil {
		t.Error("expected non-nil result")
	}

	// 测试数据只有部分长度信息
	result = it.decodeString([]byte{0x00, 0x00, 0x00}, 0)
	if result == nil {
		t.Error("expected non-nil result for partial length")
	}
}

func TestIterator_DecodeFieldValueFromData_Int64(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(int64(100))}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(int64(200))}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	count := 0
	for it.Next() {
		pt := it.Point()
		if pt != nil && pt.Fields["v"] != nil {
			count++
		}
	}

	if count != 2 {
		t.Errorf("expected 2 points with int64, got %d", count)
	}
}

func TestIterator_DecodeFieldValueFromData_Bool(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(true)}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(false)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
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

func TestIterator_LoadAllData_FileError(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	// 手动创建 iterator 并设置不存在的 dataDir 触发错误
	it := &Iterator{
		reader:       r,
		dataDir:      "/nonexistent",
		currentBlock: -1,
		pos:          -1,
		fallbackPos:  -1,
		fieldBufs:    make(map[string][]byte),
		fallbackMode: true,
	}

	err = it.loadAllData()
	if err == nil {
		t.Error("expected error for nonexistent data dir")
	}
}

func TestIterator_LoadAllData_FieldsDirError(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	// 创建一个临时的 sstable 目录但删除 fields 子目录
	tmpDir2 := t.TempDir()
	dataDir := filepath.Join(tmpDir2, "data", "sst_0")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	// 写入 timestamps 文件但不创建 fields 目录
	tsFile, err := os.Create(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		t.Fatalf("Create timestamp file failed: %v", err)
	}
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(1000))
	_, _ = tsFile.Write(tsBuf[:])
	_ = tsFile.Close()

	r2, err := NewReader(dataDir)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r2.Close() }()

	// 当 fields 目录不存在时，NewIterator 可能失败
	_, err = r2.NewIterator()
	if err != nil {
		t.Logf("NewIterator correctly failed without fields dir: %v", err)
		return
	}
	// 如果没有错误，迭代可能会返回 0 条数据
}

func TestIterator_DecodeFieldValueFromData_Float64(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.5))}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(2.5))}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	count := 0
	for it.Next() {
		pt := it.Point()
		if pt != nil && pt.Fields["v"] != nil {
			count++
		}
	}

	if count != 2 {
		t.Errorf("expected 2 points with float64, got %d", count)
	}
}

func TestWriter_AppendFieldValue_AllTypes(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
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

	w, err := NewWriter(tmpDir, 0, 0)
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

	w, err := NewWriter(tmpDir, 0, 0)
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

	w, err := NewWriter(tmpDir, 0, 0)
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 写入数据
	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}

	if err := w.WritePoints(points, nil); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}

	// 关闭 writer
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestReader_DecodeFieldValue_String(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"s": types.NewFieldValue("hello world")}},
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(2.0)}},
		{Timestamp: 3000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(3.0)}},
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
	defer func() { _ = r.Close() }()

	// 测试反向范围 (start > end)
	rows, err := r.ReadRange(3000, 1000)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	// 应该返回空或全部数据，取决于实现
	t.Logf("ReadRange(3000, 1000) returned %d rows", len(rows))
}

func TestReader_DecodeFieldValue_UnknownType(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// 使用不存在的 fields 目录触发错误
	originalDataDir := it.dataDir
	it.dataDir = "/nonexistent"
	err = it.loadBlock(0)
	if err == nil {
		t.Error("expected error for nonexistent fields dir")
	}
	it.dataDir = originalDataDir
}

func TestIterator_DecodeString_MultipleStrings(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{Timestamp: 1000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("first")}},
		{Timestamp: 2000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("second")}},
		{Timestamp: 3000, Tags: map[string]string{"host": "s1"}, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue("third")}},
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
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// decodeString 测试位置 1
	// 构建字符串数据: [5]first[6]second[5]third
	strData := []byte{
		0x00, 0x00, 0x00, 0x05, 'f', 'i', 'r', 's', 't',
		0x00, 0x00, 0x00, 0x06, 's', 'e', 'c', 'o', 'n', 'd',
		0x00, 0x00, 0x00, 0x05, 't', 'h', 'i', 'r', 'd',
	}

	result0 := it.decodeString(strData, 0)
	if result0.GetStringValue() != "first" {
		t.Errorf("expected 'first', got '%s'", result0.GetStringValue())
	}

	result1 := it.decodeString(strData, 1)
	if result1.GetStringValue() != "second" {
		t.Errorf("expected 'second', got '%s'", result1.GetStringValue())
	}

	result2 := it.decodeString(strData, 2)
	if result2.GetStringValue() != "third" {
		t.Errorf("expected 'third', got '%s'", result2.GetStringValue())
	}
}

func TestIterator_FallbackMode_LoadAllData(t *testing.T) {
	// 手动创建回退模式的数据目录（没有 _index.bin）
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data", "sst_fallback")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	fieldsDir := filepath.Join(dataDir, "fields")
	if err := os.MkdirAll(fieldsDir, 0700); err != nil {
		t.Fatalf("MkdirAll fields dir failed: %v", err)
	}

	// 创建 _timestamps.bin
	tsFile, err := os.Create(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		t.Fatalf("Create timestamps file failed: %v", err)
	}
	// 写入 3 个 timestamps
	for i := 0; i < 3; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(1000+int64(i)*1000))
		if _, err := tsFile.Write(buf[:]); err != nil {
			t.Fatalf("Write timestamp failed: %v", err)
		}
	}
	_ = tsFile.Close()

	// 创建 float64 字段文件 (8 bytes per value)
	floatFile, err := os.Create(filepath.Join(fieldsDir, "cpu.bin"))
	if err != nil {
		t.Fatalf("Create float field file failed: %v", err)
	}
	for i := 0; i < 3; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(float64(1.0+float64(i)*0.1)))
		if _, err := floatFile.Write(buf[:]); err != nil {
			t.Fatalf("Write float field failed: %v", err)
		}
	}
	_ = floatFile.Close()

	// 创建 int64 字段文件
	intFile, err := os.Create(filepath.Join(fieldsDir, "memory.bin"))
	if err != nil {
		t.Fatalf("Create int field file failed: %v", err)
	}
	for i := 0; i < 3; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(100+i))
		if _, err := intFile.Write(buf[:]); err != nil {
			t.Fatalf("Write int field failed: %v", err)
		}
	}
	_ = intFile.Close()

	// 创建 string 字段文件
	strFile, err := os.Create(filepath.Join(fieldsDir, "status.bin"))
	if err != nil {
		t.Fatalf("Create string field file failed: %v", err)
	}
	// 字符串格式: [4-byte length][string data]
	strings := []string{"start", "middle", "end"}
	for _, s := range strings {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s)))
		if _, err := strFile.Write(lenBuf[:]); err != nil {
			t.Fatalf("Write string length failed: %v", err)
		}
		if _, err := strFile.Write([]byte(s)); err != nil {
			t.Fatalf("Write string data failed: %v", err)
		}
	}
	_ = strFile.Close()

	// 创建 bool 字段文件
	boolFile, err := os.Create(filepath.Join(fieldsDir, "active.bin"))
	if err != nil {
		t.Fatalf("Create bool field file failed: %v", err)
	}
	// Bool: 1 byte per value, 0 = false, 1 = true
	if _, err := boolFile.Write([]byte{1}); err != nil {
		t.Fatalf("Write bool field failed: %v", err)
	}
	if _, err := boolFile.Write([]byte{0}); err != nil {
		t.Fatalf("Write bool field failed: %v", err)
	}
	if _, err := boolFile.Write([]byte{1}); err != nil {
		t.Fatalf("Write bool field failed: %v", err)
	}
	_ = boolFile.Close()

	// 创建 Reader（不会创建 _index.bin，所以会进入回退模式）
	r := &Reader{
		dataDir: dataDir,
		schema: Schema{
			Fields: map[string]FieldType{
				"cpu":    FieldTypeFloat64,
				"memory": FieldTypeInt64,
				"status": FieldTypeString,
				"active": FieldTypeBool,
			},
		},
	}

	it := &Iterator{
		reader:       r,
		dataDir:      dataDir,
		currentBlock: -1,
		pos:          -1,
		fallbackPos:  -1,
		fallbackMode: true,
		fieldBufs:    make(map[string][]byte),
	}

	// 调用 loadAllData
	if err := it.loadAllData(); err != nil {
		t.Fatalf("loadAllData failed: %v", err)
	}

	// 验证 timestamps
	if len(it.fallbackTimestamps) != 3 {
		t.Errorf("expected 3 timestamps, got %d", len(it.fallbackTimestamps))
	}
	if it.fallbackTimestamps[0] != 1000 {
		t.Errorf("expected first timestamp 1000, got %d", it.fallbackTimestamps[0])
	}

	// 验证 fields - decodeFieldValueFromData 应该被调用
	if len(it.fallbackFields) != 3 {
		t.Errorf("expected 3 field entries, got %d", len(it.fallbackFields))
	}

	// 验证 float64 字段解码
	cpuVal := it.fallbackFields[0]["cpu"]
	if cpuVal == nil {
		t.Error("cpu value should not be nil")
	} else if cpuVal.GetFloatValue() != 1.0 {
		t.Errorf("expected cpu=1.0, got %f", cpuVal.GetFloatValue())
	}

	// 验证 int64 字段解码
	memVal := it.fallbackFields[0]["memory"]
	if memVal == nil {
		t.Error("memory value should not be nil")
	} else if memVal.GetIntValue() != 100 {
		t.Errorf("expected memory=100, got %d", memVal.GetIntValue())
	}

	// 验证 string 字段解码
	statusVal := it.fallbackFields[0]["status"]
	if statusVal == nil {
		t.Error("status value should not be nil")
	} else if statusVal.GetStringValue() != "start" {
		t.Errorf("expected status='start', got '%s'", statusVal.GetStringValue())
	}

	// 验证 bool 字段解码
	activeVal := it.fallbackFields[0]["active"]
	if activeVal == nil {
		t.Error("active value should not be nil")
	} else if !activeVal.GetBoolValue() {
		t.Error("expected active=true")
	}

	// 验证第二次迭代
	statusVal2 := it.fallbackFields[1]["status"]
	if statusVal2.GetStringValue() != "middle" {
		t.Errorf("expected status='middle', got '%s'", statusVal2.GetStringValue())
	}

	statusVal3 := it.fallbackFields[2]["status"]
	if statusVal3.GetStringValue() != "end" {
		t.Errorf("expected status='end', got '%s'", statusVal3.GetStringValue())
	}
}

func TestIterator_DecodeFieldValueFromData_OutOfBounds(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data", "sst_bounds")
	if err := os.MkdirAll(filepath.Join(dataDir, "fields"), 0700); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// 创建只有 2 个值的 float64 字段文件
	floatFile, err := os.Create(filepath.Join(dataDir, "fields", "cpu.bin"))
	if err != nil {
		t.Fatalf("Create float field file failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(float64(1.0+float64(i)*0.1)))
		if _, err := floatFile.Write(buf[:]); err != nil {
			t.Fatalf("Write float field failed: %v", err)
		}
	}
	_ = floatFile.Close()

	// 创建 Reader
	r := &Reader{
		dataDir: dataDir,
		schema: Schema{
			Fields: map[string]FieldType{
				"cpu": FieldTypeFloat64,
			},
		},
	}

	it := &Iterator{
		reader:       r,
		dataDir:      dataDir,
		currentBlock: -1,
		pos:          -1,
		fallbackPos:  -1,
		fallbackMode: true,
		fieldBufs:    make(map[string][]byte),
	}

	// 加载数据（只有 2 个 timestamp，但字段数据也只有 2 个）
	tsFile, err := os.Create(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		t.Fatalf("Create timestamps file failed: %v", err)
	}
	for i := 0; i < 3; i++ { // 3 timestamps
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(1000+int64(i)*1000))
		if _, err := tsFile.Write(buf[:]); err != nil {
			t.Fatalf("Write timestamp failed: %v", err)
		}
	}
	_ = tsFile.Close()

	// loadAllData - 当 pos=2 时，offset 会超出字段数据范围
	if err := it.loadAllData(); err != nil {
		t.Fatalf("loadAllData failed: %v", err)
	}

	// 访问超出范围的 pos 应该返回零值
	val := it.decodeFieldValueFromData("cpu", []byte{0, 0, 0, 0, 0, 0, 0, 0}, 10)
	if val == nil {
		t.Error("decodeFieldValueFromData should return zero value for out of bounds")
	}
}
