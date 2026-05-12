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
}

// NewIterator 创建新的流式迭代器。
func (r *Reader) NewIterator() (*Iterator, error) {
	it := &Iterator{
		reader:       r,
		currentBlock: -1,
		pos:          -1,
	}

	if r.blockIndex != nil {
		idx := r.blockIndex
		it.blockIndex = make([]BlockIndexEntry, idx.Len())
		for i := 0; i < idx.Len(); i++ {
			it.blockIndex[i] = idx.Entry(i)
		}
	}

	return it, nil
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
