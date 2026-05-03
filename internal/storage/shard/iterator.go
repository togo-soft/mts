package shard

import (
	"micro-ts/internal/storage/shard/sstable"
	"micro-ts/internal/types"
)

// ShardIterator 单个 Shard 的迭代器
type ShardIterator struct {
	shard     *Shard
	startTime int64 // 查询起始时间（包含）
	endTime   int64 // 查询结束时间（不包含）

	memIter *MemTableIterator   // MemTable 迭代器
	sstIter *sstable.Iterator // SSTable 迭代器

	// 当前 peek
	memRow *types.PointRow
	sstRow *types.PointRow
}

// NewShardIterator 创建 Shard 迭代器（带时间范围过滤）
func NewShardIterator(shard *Shard, startTime, endTime int64) *ShardIterator {
	si := &ShardIterator{
		shard:     shard,
		startTime: startTime,
		endTime:   endTime,
	}

	// 创建 MemTable 迭代器
	si.memIter = shard.memTable.Iterator()
	if si.memIter.Next() {
		p := si.memIter.Point()
		si.memRow = si.pointToRow(p)
	}

	// 创建 SSTable 迭代器（只在该 shard 有 SSTable 数据时才创建）
	sstReader, err := sstable.NewReader(shard.dir)
	if err == nil {
		sstIter, iterErr := sstReader.NewIterator()
		if iterErr == nil && sstIter != nil {
			si.sstIter = sstIter
			if si.sstIter.Next() {
				si.sstRow = si.sstIter.Point()
			}
		}
	}

	return si
}

// pointToRow 将 Point 转换为 PointRow
func (si *ShardIterator) pointToRow(p *types.Point) *types.PointRow {
	if p == nil {
		return nil
	}
	return &types.PointRow{
		SID:       0,
		Timestamp: p.Timestamp,
		Tags:      p.Tags,
		Fields:    p.Fields,
	}
}

// Next 返回下一个有序点（按 timestamp 升序），过滤时间范围
func (si *ShardIterator) Next() *types.PointRow {
	for {
		// 如果 MemTable 和 SSTable 都有数据，取 timestamp 较小的
		if si.memRow != nil && si.sstRow != nil {
			if si.memRow.Timestamp < si.sstRow.Timestamp {
				row := si.memRow
				si.memRow = si.nextMemRow()
				return si.filterRow(row)
			}
			// memRow.Timestamp >= sstRow.Timestamp（包括相等）
			row := si.sstRow
			si.sstRow = si.nextSstRow()
			return si.filterRow(row)
		}

		// 只剩 MemTable
		if si.memRow != nil {
			row := si.memRow
			si.memRow = si.nextMemRow()
			return si.filterRow(row)
		}

		// 只剩 SSTable
		if si.sstRow != nil {
			row := si.sstRow
			si.sstRow = si.nextSstRow()
			return si.filterRow(row)
		}

		// 都耗尽了
		return nil
	}
}

// filterRow 检查 row 是否在时间范围内
func (si *ShardIterator) filterRow(row *types.PointRow) *types.PointRow {
	if row == nil {
		return nil
	}
	if row.Timestamp >= si.startTime && (si.endTime <= 0 || row.Timestamp < si.endTime) {
		return row
	}
	// 不在范围内，继续获取下一个
	return si.Next()
}

// nextMemRow 获取下一个 MemTable row
func (si *ShardIterator) nextMemRow() *types.PointRow {
	for si.memIter.Next() {
		p := si.memIter.Point()
		if p.Timestamp >= si.startTime && (si.endTime <= 0 || p.Timestamp < si.endTime) {
			return si.pointToRow(p)
		}
	}
	return nil
}

// nextSstRow 获取下一个 SSTable row
func (si *ShardIterator) nextSstRow() *types.PointRow {
	for si.sstIter != nil && si.sstIter.Next() {
		row := si.sstIter.Point()
		if row.Timestamp >= si.startTime && (si.endTime <= 0 || row.Timestamp < si.endTime) {
			return row
		}
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