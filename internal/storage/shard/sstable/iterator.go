package sstable

import (
	"encoding/binary"
	"sort"

	"codeberg.org/micro-ts/mts/types"
)

// Iterator 是 SSTable 的流式迭代器。
type Iterator struct {
	reader *Reader

	blockIndex   []BlockIndexEntry
	currentBlock int

	blockTimestamps []int64
	blockSids       []uint64
	fieldBufs       map[string][]byte
	blockRowCount   int
	pos             int

	fallbackMode       bool
	fallbackTimestamps []int64
	fallbackSids       []uint64
	fallbackFields     []map[string]*types.FieldValue
	fallbackPos        int
}

// NewIterator 创建新的流式迭代器。
func (r *Reader) NewIterator() (*Iterator, error) {
	it := &Iterator{
		reader:       r,
		currentBlock: -1,
		pos:          -1,
		fallbackPos:  -1,
		fieldBufs:    make(map[string][]byte),
	}

	if r.HasBlockIndex() {
		idx := r.GetBlockIndex()
		it.blockIndex = make([]BlockIndexEntry, idx.Len())
		for i := 0; i < idx.Len(); i++ {
			it.blockIndex[i] = idx.Entry(i)
		}
	} else {
		it.fallbackMode = true
		if err := it.loadAllData(); err != nil {
			return nil, err
		}
	}

	return it, nil
}

// loadAllData 回退模式下加载所有数据。
func (it *Iterator) loadAllData() error {
	tsOffset, tsSize, _ := it.reader.sectionTable.LookupByType(SectionTimestamps)
	tsData := make([]byte, tsSize)
	if _, err := it.reader.file.ReadAt(tsData, int64(tsOffset)); err != nil {
		return err
	}
	timestamps := make([]int64, 0, len(tsData)/8)
	for i := 0; i+8 <= len(tsData); i += 8 {
		timestamps = append(timestamps, int64(binary.BigEndian.Uint64(tsData[i:i+8])))
	}
	if len(timestamps) == 0 {
		return nil
	}

	sids, err := it.reader.readSids(len(timestamps))
	if err != nil {
		return err
	}
	it.fallbackSids = sids

	fieldNames := it.reader.sectionTable.FieldNames()
	fieldData := make(map[string][]byte)
	for _, name := range fieldNames {
		fOffset, fSize := it.reader.sectionTable.Lookup(name)
		if fSize == 0 {
			continue
		}
		data := make([]byte, fSize)
		if _, err := it.reader.file.ReadAt(data, int64(fOffset)); err != nil {
			return err
		}
		fieldData[name] = data
	}

	it.fallbackTimestamps = timestamps
	it.fallbackFields = make([]map[string]*types.FieldValue, len(timestamps))
	for i := 0; i < len(timestamps); i++ {
		row := make(map[string]*types.FieldValue)
		for _, name := range fieldNames {
			row[name] = it.decodeFieldValueFromData(name, fieldData[name], i)
		}
		it.fallbackFields[i] = row
	}

	return nil
}

// decodeFieldValueFromData 从原始数据中解码字段值（用于无索引回退模式）。
func (it *Iterator) decodeFieldValueFromData(name string, data []byte, pos int) *types.FieldValue {
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

// SeekToTime 定位到指定时间的 Block。
func (it *Iterator) SeekToTime(target int64) error {
	if len(it.blockIndex) == 0 {
		return nil
	}

	blockIdx := sort.Search(len(it.blockIndex), func(i int) bool {
		return it.blockIndex[i].LastTimestamp >= target
	})

	if blockIdx >= len(it.blockIndex) {
		it.currentBlock = len(it.blockIndex)
		return nil
	}

	it.currentBlock = blockIdx
	return it.loadBlock(blockIdx)
}
