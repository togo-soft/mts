package sstable

import (
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
		Fields:    make(map[string]*types.FieldValue),
	}
	if it.pos < len(it.blockSids) {
		row.Sid = it.blockSids[it.pos]
	}

	// 惰性解码：首次访问字段时解码全部行并缓存
	if it.blockFieldValues == nil {
		it.blockFieldValues = make(map[string][]*types.FieldValue)
	}
	for name, rawData := range it.blockFieldData {
		if _, ok := it.blockFieldValues[name]; !ok {
			vals, err := it.reader.decodeFieldSectionBlockFromData(name, rawData, it.blockRowCount)
			if err != nil {
				it.blockFieldValues[name] = nil
				continue
			}
			it.blockFieldValues[name] = vals
		}
		if vals := it.blockFieldValues[name]; vals != nil && it.pos < len(vals) {
			row.Fields[name] = vals[it.pos]
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
