package sstable

import (
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
		row := &types.PointRow{
			Timestamp: it.fallbackTimestamps[it.fallbackPos],
			Fields:    it.fallbackFields[it.fallbackPos],
		}
		if it.fallbackPos < len(it.fallbackSids) {
			row.Sid = it.fallbackSids[it.fallbackPos]
		}
		return row
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
	if it.pos < len(it.blockSids) {
		row.Sid = it.blockSids[it.pos]
	}

	for name, vals := range it.blockFieldValues {
		if it.pos < len(vals) {
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
