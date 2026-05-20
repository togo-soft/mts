package sstable

// loadBlock 按 block 独立解码指定块的数据。
func (it *Iterator) loadBlock(blockIdx int) error {
	if blockIdx < 0 || blockIdx >= len(it.blockIndex) {
		return nil
	}

	// ZoneMap 跳过检查：过滤条件明确此块不可能包含匹配数据时跳过
	if it.zoneMapIndex != nil && len(it.filterConds) > 0 {
		if it.shouldSkipBlock(blockIdx) {
			it.blockRowCount = 0
			return nil
		}
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

	// 清除上一 block 的字段缓存
	it.blockFieldData = nil
	it.blockFieldValues = nil

	// 确定需要解压的字段
	fieldNames := it.projectedFields
	if fieldNames == nil {
		fieldNames = it.reader.sectionTable.FieldNames()
	}

	// 仅解压原始字节，不解码
	it.blockFieldData = make(map[string][]byte, len(fieldNames))
	it.blockFieldOrder = fieldNames
	for _, name := range fieldNames {
		data, err := it.reader.readFieldBlockRaw(name, blockIdx)
		if err != nil {
			return err
		}
		it.blockFieldData[name] = data
	}

	return nil
}
