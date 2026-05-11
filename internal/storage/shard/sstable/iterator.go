package sstable

import (
	"sort"

	"codeberg.org/micro-ts/mts/types"
)

// Iterator 是 SSTable 的流式迭代器。
type Iterator struct {
	reader *Reader

	blockIndex   []BlockIndexEntry
	currentBlock int

	blockTimestamps  []int64
	blockSids        []uint64
	blockFieldValues map[string][]*types.FieldValue
	blockRowCount    int
	pos              int

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

// loadAllData 回退模式下加载所有数据（使用编码感知的解码器）。
func (it *Iterator) loadAllData() error {
	timestamps, err := it.reader.readTimestamps()
	if err != nil {
		return err
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
	decodedFields, err := it.reader.ReadAllDecodedFieldSections(fieldNames, len(timestamps))
	if err != nil {
		return err
	}

	it.fallbackTimestamps = timestamps
	it.fallbackFields = make([]map[string]*types.FieldValue, len(timestamps))
	for i := 0; i < len(timestamps); i++ {
		row := make(map[string]*types.FieldValue)
		for _, name := range fieldNames {
			if vals, ok := decodedFields[name]; ok && i < len(vals) {
				row[name] = vals[i]
			}
		}
		it.fallbackFields[i] = row
	}

	return nil
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
