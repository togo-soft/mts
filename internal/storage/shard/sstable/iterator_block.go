package sstable

import (
	"codeberg.org/micro-ts/mts/types"
)

// loadBlock 按 block 独立解码指定块的数据。
func (it *Iterator) loadBlock(blockIdx int) error {
	if blockIdx < 0 || blockIdx >= len(it.blockIndex) {
		return nil
	}

	entry := it.blockIndex[blockIdx]
	it.currentBlock = blockIdx
	it.blockRowCount = int(entry.RowCount)

	ts, err := it.reader.readTimestampsBlock(blockIdx)
	if err != nil {
		return err
	}
	it.blockTimestamps = ts

	sids, err := it.reader.readSidsBlock(blockIdx)
	if err != nil {
		return err
	}
	it.blockSids = sids

	fieldNames := it.reader.sectionTable.FieldNames()
	it.blockFieldValues = make(map[string][]*types.FieldValue, len(fieldNames))
	for _, name := range fieldNames {
		vals, err := it.reader.decodeFieldSectionBlock(name, blockIdx)
		if err != nil {
			return err
		}
		it.blockFieldValues[name] = vals
	}

	return nil
}
