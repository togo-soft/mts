package shard

import (
	"sync"

	"codeberg.org/micro-ts/mts/types"
)

// ShardIterator 是单个 Shard 的数据迭代器。
//
// 功能：
//
//   - 合并 MemTable 和 SSTable 的数据源
//   - 按时间戳升序返回数据
//   - 支持时间范围过滤
//
// 线程安全：
//
//	ShardIterator 是线程安全的。
//	可以在多个 goroutine 之间共享同一个 ShardIterator。
//	内部使用读写锁保护所有状态操作，允许多个并发读。
//
// 字段说明：
//
//   - shard:     所属的 Shard
//   - startTime: 查询起始时间（包含）
//   - endTime:   查询结束时间（不包含）
//   - memIter:   MemTable 迭代器
//   - rows:      SSTable 预加载的数据
//   - rowIdx:    当前在 rows 中的位置
//
// 性能考虑：
//
// SSTable 数据在创建时一次性加载。对于大数据集，应考虑流式读取优化。
// 当前实现适合 SSTable 较小的场景。
type ShardIterator struct {
	shard     *Shard
	startTime int64 // 查询起始时间（包含）
	endTime   int64 // 查询结束时间（不包含）

	memIter *MemTableIterator // MemTable 迭代器
	rows    []*types.PointRow // SSTable 预读取的数据
	rowIdx  int              // 当前在 rows 中的位置
	err     error            // 迭代过程中的错误

	// 当前 peek
	memRow *types.PointRow
	sstRow *types.PointRow

	// 线程安全保护 - 读写锁允许多个并发读
	mu sync.RWMutex
}

// NewShardIterator 创建 Shard 迭代器（带时间范围过滤）。
//
// 参数：
//   - shard:     目标 Shard
//   - startTime: 查询起始时间（包含）
//   - endTime:   查询截止时间（不包含），<=0 表示无限制
//
// 返回：
//   - *ShardIterator: 初始化后的迭代器
//
// 初始化过程：
//
//  1. 创建 MemTable 迭代器并定位到第一条记录
//  2. 预加载 SSTable 中时间范围内的数据
//  3. 记录当前位置用于归并排序
//
// 注意：
//
//	SSTable 数据在创建时一次性加载，对于大数据集可能消耗较多内存。
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

	// 从 SSTable 预读取数据
	rows, err := shard.readFromSSTable(startTime, endTime)
	if err != nil {
		si.err = err
		return si
	}
	if len(rows) > 0 {
		si.rows = rows
		si.rowIdx = 0
		si.sstRow = si.rows[0]
	}

	return si
}

// pointToRow 将 Point 转换为 PointRow
func (si *ShardIterator) pointToRow(p *types.Point) *types.PointRow {
	if p == nil {
		return nil
	}
	return &types.PointRow{
		Sid:       0,
		Timestamp: p.Timestamp,
		Tags:      p.Tags,
		Fields:    p.Fields,
	}
}

// Next 返回下一个匹配时间范围的数据点。
//
// 返回：
//   - *types.PointRow: 下一个数据点，如果耗尽返回 nil
//
// 归并排序：
//
//	比较 MemTable 和 SSTable 的当前数据，返回时间戳较小者。
//	返回后自动推进对应数据源的迭代器。
//
// 过滤：
//
//	自动过滤不在 [startTime, endTime) 范围内的数据。
//	如果当前数据超出范围，自动跳过并获取下一个。
func (si *ShardIterator) Next() *types.PointRow {
	si.mu.Lock()
	defer si.mu.Unlock()

	// 如果 MemTable 和 SSTable 都有数据，取 timestamp 较小的
	if si.memRow != nil && si.sstRow != nil {
		if si.memRow.Timestamp < si.sstRow.Timestamp {
			row := si.memRow
			si.memRow = si.nextMemRowLocked()
			return si.filterRowLocked(row)
		}
		// memRow.Timestamp >= sstRow.Timestamp（包括相等）
		row := si.sstRow
		si.sstRow = si.nextSstRowLocked()
		return si.filterRowLocked(row)
	}

	// 只剩 MemTable
	if si.memRow != nil {
		row := si.memRow
		si.memRow = si.nextMemRowLocked()
		return si.filterRowLocked(row)
	}

	// 只剩 SSTable
	if si.sstRow != nil {
		row := si.sstRow
		si.sstRow = si.nextSstRowLocked()
		return si.filterRowLocked(row)
	}

	// 都耗尽了
	return nil
}

// filterRow 检查 row 是否在时间范围内
func (si *ShardIterator) filterRow(row *types.PointRow) *types.PointRow {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.filterRowLocked(row)
}

// filterRowLocked 检查 row 是否在时间范围内（已持有锁）
func (si *ShardIterator) filterRowLocked(row *types.PointRow) *types.PointRow {
	if row == nil {
		return nil
	}
	if row.Timestamp >= si.startTime && (si.endTime <= 0 || row.Timestamp < si.endTime) {
		return row
	}
	// 不在范围内，继续获取下一个
	return si.nextLocked()
}

// nextLocked 获取下一个内部方法（已持有锁）
func (si *ShardIterator) nextLocked() *types.PointRow {
	// 如果 MemTable 和 SSTable 都有数据，取 timestamp 较小的
	if si.memRow != nil && si.sstRow != nil {
		if si.memRow.Timestamp < si.sstRow.Timestamp {
			row := si.memRow
			si.memRow = si.nextMemRowLocked()
			return si.filterRowLocked(row)
		}
		row := si.sstRow
		si.sstRow = si.nextSstRowLocked()
		return si.filterRowLocked(row)
	}

	// 只剩 MemTable
	if si.memRow != nil {
		row := si.memRow
		si.memRow = si.nextMemRowLocked()
		return si.filterRowLocked(row)
	}

	// 只剩 SSTable
	if si.sstRow != nil {
		row := si.sstRow
		si.sstRow = si.nextSstRowLocked()
		return si.filterRowLocked(row)
	}

	return nil
}

// nextMemRow 获取下一个 MemTable row（已持有锁）
func (si *ShardIterator) nextMemRowLocked() *types.PointRow {
	for si.memIter.Next() {
		p := si.memIter.Point()
		if p.Timestamp >= si.startTime && (si.endTime <= 0 || p.Timestamp < si.endTime) {
			return si.pointToRow(p)
		}
	}
	return nil
}

// nextSstRow 获取下一个 SSTable row（已持有锁）
func (si *ShardIterator) nextSstRowLocked() *types.PointRow {
	si.rowIdx++
	if si.rowIdx < len(si.rows) {
		row := si.rows[si.rowIdx]
		if row.Timestamp >= si.startTime && (si.endTime <= 0 || row.Timestamp < si.endTime) {
			return row
		}
		return si.nextSstRowLocked()
	}
	return nil
}

// Current 返回当前位置的数据点（不推进迭代器）。
//
// 返回：
//   - *types.PointRow: 当前数据点
//
// 归并逻辑：
//
//	比较 MemTable 和 SSTable 的当前数据，返回时间戳较小者。
//	相等的优先返回 SSTable 数据。
//
// 使用场景：
//
//	用于 peek 操作，在决定推进哪个数据源前查看当前值。
//	QueryIterator 使用此方法构建最小堆。
func (si *ShardIterator) Current() *types.PointRow {
	si.mu.RLock()
	defer si.mu.RUnlock()

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

// Err 返回迭代过程中发生的错误。
//
// 返回：
//   - error: 迭代错误，如果无错误返回 nil
func (si *ShardIterator) Err() error {
	return si.err
}
