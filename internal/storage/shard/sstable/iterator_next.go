package sstable

import (
	"encoding/binary"
	"math"

	"codeberg.org/micro-ts/mts/types"
)

// Next 移动到下一个数据点。
func (it *Iterator) Next() bool {
	if it.fallbackMode {
		it.fallbackPos++
		return it.fallbackPos < len(it.fallbackTimestamps)
	}

	if it.currentBlock < 0 {
		if len(it.blockIndex) == 0 {
			return false
		}
		if err := it.loadBlock(0); err != nil {
			return false
		}
	}

	it.pos++
	if it.pos >= it.blockRowCount {
		it.currentBlock++
		if it.currentBlock >= len(it.blockIndex) {
			return false
		}
		if err := it.loadBlock(it.currentBlock); err != nil {
			return false
		}
		it.pos = 0
	}

	return it.pos < it.blockRowCount
}

// Point 返回当前迭代位置的数据点。
func (it *Iterator) Point() *types.PointRow {
	if it.fallbackMode {
		if it.fallbackPos < 0 || it.fallbackPos >= len(it.fallbackTimestamps) {
			return nil
		}
		return &types.PointRow{
			Timestamp: it.fallbackTimestamps[it.fallbackPos],
			Fields:    it.fallbackFields[it.fallbackPos],
		}
	}

	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return nil
	}
	if it.pos < 0 || it.pos >= it.blockRowCount || it.pos >= len(it.blockTimestamps) {
		return nil
	}

	row := &types.PointRow{
		Timestamp: it.blockTimestamps[it.pos],
		Fields:    make(map[string]*types.FieldValue),
	}

	for name, data := range it.fieldBufs {
		row.Fields[name] = it.decodeFieldValue(name, data, it.pos)
	}

	return row
}

// decodeFieldValue 解码字段值
func (it *Iterator) decodeFieldValue(name string, data []byte, pos int) *types.FieldValue {
	fieldType := it.reader.schema.Fields[name]
	fixedSize := it.fieldFixedSize(fieldType)

	if fixedSize > 0 {
		offset := pos * fixedSize
		if offset+fixedSize > len(data) {
			return it.zeroValue(fieldType)
		}
		return it.decodeFixedValue(data[offset:offset+fixedSize], fieldType)
	}

	return it.decodeString(data, pos)
}

// fieldFixedSize 返回字段的固定大小，-1 表示变长
func (it *Iterator) fieldFixedSize(t FieldType) int {
	switch t {
	case FieldTypeFloat64, FieldTypeInt64:
		return 8
	case FieldTypeBool:
		return 1
	case FieldTypeString:
		return -1
	default:
		return 8
	}
}

// decodeFixedValue 解码固定大小字段
func (it *Iterator) decodeFixedValue(data []byte, t FieldType) *types.FieldValue {
	switch t {
	case FieldTypeFloat64:
		bits := binary.BigEndian.Uint64(data)
		return types.NewFieldValue(math.Float64frombits(bits))
	case FieldTypeInt64:
		bits := binary.BigEndian.Uint64(data)
		return types.NewFieldValue(int64(bits))
	case FieldTypeBool:
		if len(data) > 0 && data[0] != 0 {
			return types.NewFieldValue(true)
		}
		return types.NewFieldValue(false)
	default:
		bits := binary.BigEndian.Uint64(data)
		return types.NewFieldValue(bits)
	}
}

// decodeString 解码字符串字段
func (it *Iterator) decodeString(data []byte, pos int) *types.FieldValue {
	offset := 0
	for i := 0; i < pos; i++ {
		if offset+4 > len(data) {
			return types.NewFieldValue("")
		}
		strLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4 + strLen
	}

	if offset+4 > len(data) {
		return types.NewFieldValue("")
	}

	strLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if offset+strLen > len(data) {
		return types.NewFieldValue(string(data[offset:]))
	}

	return types.NewFieldValue(string(data[offset : offset+strLen]))
}

// zeroValue 返回类型的零值
func (it *Iterator) zeroValue(t FieldType) *types.FieldValue {
	switch t {
	case FieldTypeFloat64:
		return types.NewFieldValue(float64(0))
	case FieldTypeInt64:
		return types.NewFieldValue(int64(0))
	case FieldTypeBool:
		return types.NewFieldValue(false)
	case FieldTypeString:
		return types.NewFieldValue("")
	default:
		return types.NewFieldValue(float64(0))
	}
}

// CurrentBlockFirstTimestamp 返回当前 Block 的起始时间。
func (it *Iterator) CurrentBlockFirstTimestamp() int64 {
	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return 0
	}
	return it.blockIndex[it.currentBlock].FirstTimestamp
}

// CurrentBlockLastTimestamp 返回当前 Block 的结束时间。
func (it *Iterator) CurrentBlockLastTimestamp() int64 {
	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return 0
	}
	return it.blockIndex[it.currentBlock].LastTimestamp
}

// Done 返回是否已经遍历完所有数据。
func (it *Iterator) Done() bool {
	return it.currentBlock >= len(it.blockIndex)
}
