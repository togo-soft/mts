// internal/storage/shard/iterator.go
package shard

import (
	"micro-ts/internal/storage/shard/sstable"
	"micro-ts/internal/types"
)

// ShardIterator 单个 Shard 的迭代器
type ShardIterator struct {
	shard   *Shard
	memIter *MemTableIterator // MemTable 迭代器
	sstIter *sstable.Iterator // SSTable 迭代器

	// 当前 peek
	memRow *types.PointRow
	sstRow *types.PointRow
}

// NewShardIterator 创建 Shard 迭代器
func NewShardIterator(shard *Shard) *ShardIterator {
	si := &ShardIterator{
		shard: shard,
	}

	// 创建 MemTable 迭代器
	si.memIter = shard.memTable.Iterator()
	if si.memIter.Next() {
		p := si.memIter.Point()
		si.memRow = &types.PointRow{
			Timestamp: p.Timestamp,
			Tags:      p.Tags,
			Fields:    p.Fields,
		}
	}

	// 创建 SSTable 迭代器
	sstReader, err := sstable.NewReader(shard.dir)
	if err == nil {
		si.sstIter, _ = sstReader.NewIterator()
		if si.sstIter != nil && si.sstIter.Next() {
			si.sstRow = si.sstIter.Point()
		}
	}

	return si
}

// Next 返回下一个有序点（按 timestamp 升序）
func (si *ShardIterator) Next() *types.PointRow {
	// 如果 MemTable 和 SSTable 都有数据，取 timestamp 较小的
	if si.memRow != nil && si.sstRow != nil {
		if si.memRow.Timestamp < si.sstRow.Timestamp {
			row := si.memRow
			si.memRow = si.nextMemRow()
			return row
		}
		row := si.sstRow
		si.sstRow = si.nextSstRow()
		return row
	}

	// 只剩 MemTable
	if si.memRow != nil {
		row := si.memRow
		si.memRow = si.nextMemRow()
		return row
	}

	// 只剩 SSTable
	if si.sstRow != nil {
		row := si.sstRow
		si.sstRow = si.nextSstRow()
		return row
	}

	// 都耗尽了
	return nil
}

func (si *ShardIterator) nextMemRow() *types.PointRow {
	if si.memIter.Next() {
		p := si.memIter.Point()
		return &types.PointRow{
			Timestamp: p.Timestamp,
			Tags:      p.Tags,
			Fields:    p.Fields,
		}
	}
	return nil
}

func (si *ShardIterator) nextSstRow() *types.PointRow {
	if si.sstIter != nil && si.sstIter.Next() {
		return si.sstIter.Point()
	}
	return nil
}

// Current 返回当前 peek 的 row（不推进）
func (si *ShardIterator) Current() *types.PointRow {
	if si.memRow != nil && si.sstRow != nil {
		if si.memRow.Timestamp < si.sstRow.Timestamp {
			return si.memRow
		}
		return si.sstRow
	}
	if si.memRow != nil {
		return si.memRow
	}
	return si.sstRow
}
