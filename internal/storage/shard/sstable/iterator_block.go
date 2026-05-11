package sstable

// loadBlock 加载指定 block 的数据。
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

	fieldNames := it.reader.sectionTable.FieldNames()
	it.fieldBufs = make(map[string][]byte)
	for _, name := range fieldNames {
		fieldType := it.reader.schema.Fields[name]

		var fieldSize int
		switch fieldType {
		case FieldTypeFloat64, FieldTypeInt64:
			fieldSize = 8
		case FieldTypeBool:
			fieldSize = 1
		case FieldTypeString:
			fieldSize = -1
		default:
			fieldSize = 8
		}

		if fieldSize > 0 {
			byteOffset := int(entry.Offset) * fieldSize
			byteCount := int(entry.RowCount) * fieldSize
			data, err := it.readFieldSection(name, byteOffset, byteCount)
			if err != nil {
				return err
			}
			it.fieldBufs[name] = data
		} else {
			// 变长字段（string），读取全部数据
			fOffset, fSize := it.reader.sectionTable.Lookup(name)
			if fSize > 0 {
				data := make([]byte, fSize)
				if _, err := it.reader.file.ReadAt(data, int64(fOffset)); err != nil {
					return err
				}
				it.fieldBufs[name] = data
			}
		}
	}

	return nil
}

// readFieldSection 从单文件中读取字段段的指定范围。
func (it *Iterator) readFieldSection(name string, offset, size int) ([]byte, error) {
	fOffset, _ := it.reader.sectionTable.Lookup(name)
	data := make([]byte, size)
	if _, err := it.reader.file.ReadAt(data, int64(fOffset)+int64(offset)); err != nil {
		return nil, err
	}
	return data, nil
}
