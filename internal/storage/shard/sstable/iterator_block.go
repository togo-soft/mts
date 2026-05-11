package sstable

import (
	"codeberg.org/micro-ts/mts/types"
)

// loadBlock 加载指定 block 的数据，使用解码后的字段值。
func (it *Iterator) loadBlock(blockIdx int) error {
	if blockIdx < 0 || blockIdx >= len(it.blockIndex) {
		return nil
	}

	entry := it.blockIndex[blockIdx]
	it.currentBlock = blockIdx
	it.blockRowCount = int(entry.RowCount)

	ts, err := it.reader.readTimestampRange(entry.Offset, entry.RowCount)
	if err != nil {
		return err
	}
	it.blockTimestamps = ts

	sids, err := it.reader.readSidsRange(entry.Offset, entry.RowCount)
	if err != nil {
		return err
	}
	it.blockSids = sids

	// 解码所有字段段，然后按块范围切片
	fieldNames := it.reader.sectionTable.FieldNames()
	rowCount := int(it.reader.header.RowCount)
	allFields, err := it.reader.ReadAllDecodedFieldSections(fieldNames, rowCount)
	if err != nil {
		return err
	}

	startRow := int(entry.Offset)
	endRow := startRow + it.blockRowCount

	it.blockFieldValues = make(map[string][]*types.FieldValue)
	for _, name := range fieldNames {
		vals := allFields[name]
		if startRow >= len(vals) {
			continue
		}
		if endRow > len(vals) {
			endRow = len(vals)
		}
		it.blockFieldValues[name] = vals[startRow:endRow]
	}

	return nil
}
