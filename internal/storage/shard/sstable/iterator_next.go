package sstable

import (
	"encoding/binary"
	"math"

	"codeberg.org/micro-ts/mts/types"
)

// Next 移动到下一个数据点。
func (it *Iterator) Next() bool {
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
	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return nil
	}
	if it.pos < 0 || it.pos >= it.blockRowCount || it.pos >= len(it.blockTimestamps) {
		return nil
	}

	row := &types.PointRow{
		Timestamp: it.blockTimestamps[it.pos],
	}
	if it.pos < len(it.blockSids) {
		row.Sid = it.blockSids[it.pos]
	}

	// 惰性解码：首次访问字段时解码全部行并缓存
	if it.blockFieldValues == nil {
		it.blockFieldValues = make(map[string][]*types.FieldValue)
	}
	row.Fields = make([]*types.FieldEntry, 0, len(it.blockFieldOrder))
	for _, name := range it.blockFieldOrder {
		rawData := it.blockFieldData[name]
		if _, ok := it.blockFieldValues[name]; !ok {
			vals, err := it.reader.decodeFieldSectionBlockFromData(name, rawData, it.blockRowCount)
			if err != nil {
				it.blockFieldValues[name] = nil
				continue
			}
			it.blockFieldValues[name] = vals
		}
		if vals := it.blockFieldValues[name]; vals != nil && it.pos < len(vals) {
			row.Fields = append(row.Fields, &types.FieldEntry{Key: name, Value: vals[it.pos]})
		}
	}

	return row
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

// CurrentMemPoint 直接构造 MemPoint，不分配 PointRow/FieldEntry/map。
// 复用 Iterator 内部的惰性解码缓存，直接将字段值序列化为 FieldData 格式。
func (it *Iterator) CurrentMemPoint(db, meas string) types.MemPoint {
	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return types.MemPoint{}
	}
	if it.pos < 0 || it.pos >= it.blockRowCount || it.pos >= len(it.blockTimestamps) {
		return types.MemPoint{}
	}

	ts := it.blockTimestamps[it.pos]
	var sid uint64
	if it.pos < len(it.blockSids) {
		sid = it.blockSids[it.pos]
	}

	// 触发惰性解码（首次访问时解码所有字段块）
	it.ensureFieldsDecoded()

	return types.MemPoint{
		Database:    db,
		Measurement: meas,
		Timestamp:   ts,
		Sid:         sid,
		FieldData:   it.buildFieldData(),
	}
}

// ensureFieldsDecoded 确保当前 block 的所有字段已惰性解码。
func (it *Iterator) ensureFieldsDecoded() {
	if it.blockFieldValues == nil {
		it.blockFieldValues = make(map[string][]*types.FieldValue)
	}
	for _, name := range it.blockFieldOrder {
		if _, ok := it.blockFieldValues[name]; ok {
			continue
		}
		rawData := it.blockFieldData[name]
		if rawData == nil {
			it.blockFieldValues[name] = nil
			continue
		}
		vals, err := it.reader.decodeFieldSectionBlockFromData(name, rawData, it.blockRowCount)
		if err != nil {
			it.blockFieldValues[name] = nil
			continue
		}
		it.blockFieldValues[name] = vals
	}
}

// buildFieldData 将当前位置的字段值直接序列化为 FieldData 字节流。
func (it *Iterator) buildFieldData() []byte {
	pos := it.pos

	// 第一遍：统计有效字段并计算总字节数
	type fieldItem struct {
		name string
		fv   *types.FieldValue
	}
	items := make([]fieldItem, 0, len(it.blockFieldOrder))
	totalSize := 2 // fieldCount(2B)

	for _, name := range it.blockFieldOrder {
		vals := it.blockFieldValues[name]
		if vals == nil || pos >= len(vals) {
			continue
		}
		fv := vals[pos]
		if fv == nil {
			continue
		}
		totalSize += 2 + len(name) + 1 // keyLen(2B) + key + type(1B)
		switch v := fv.GetValue().(type) {
		case *types.FieldValue_FloatValue, *types.FieldValue_IntValue:
			totalSize += 8
		case *types.FieldValue_StringValue:
			totalSize += 2 + len(v.StringValue)
		case *types.FieldValue_BoolValue:
			totalSize += 1
		}
		items = append(items, fieldItem{name, fv})
	}

	// 第二遍：写入字节
	buf := make([]byte, 0, totalSize)
	buf = appendU16BE(buf, uint16(len(items)))
	for _, item := range items {
		buf = appendU16BE(buf, uint16(len(item.name)))
		buf = append(buf, item.name...)
		switch v := item.fv.GetValue().(type) {
		case *types.FieldValue_FloatValue:
			buf = append(buf, 0)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], math.Float64bits(v.FloatValue))
			buf = append(buf, vb[:]...)
		case *types.FieldValue_IntValue:
			buf = append(buf, 1)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], uint64(v.IntValue))
			buf = append(buf, vb[:]...)
		case *types.FieldValue_StringValue:
			buf = append(buf, 2)
			buf = appendU16BE(buf, uint16(len(v.StringValue)))
			buf = append(buf, v.StringValue...)
		case *types.FieldValue_BoolValue:
			buf = append(buf, 3)
			if v.BoolValue {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	}

	return buf
}

// appendU16BE 以大端序追加一个 uint16 到 buf。
func appendU16BE(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}
