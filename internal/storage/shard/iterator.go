package shard

import (
	"fmt"
	"sync"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// ShardIterator 是单个 Shard 的数据迭代器。
//
// 功能：
//
//   - 合并 MemTable 和 SSTable 的数据源（流式读取，不预加载）
//   - 按时间戳升序返回数据
//   - 支持时间范围过滤
//
// 线程安全：
//
//	ShardIterator 是线程安全的。
//	可以在多个 goroutine 之间共享同一个 ShardIterator。
//	内部使用读写锁保护所有状态操作，允许多个并发读。
type ShardIterator struct {
	shard       *Shard
	db          string
	measurement string
	startTime   int64 // 查询起始时间（包含）
	endTime     int64 // 查询结束时间（不包含）

	memIter *memtable.MemTableIterator // MemTable 迭代器
	sstIter *sstable.MergeIterator     // SSTable 流式归并迭代器
	err     error                      // 迭代过程中的错误

	// 当前 peek
	memRow *types.PointRow
	sstRow *types.PointRow

	produced int // 已输出的行数
	maxRows  int // 最大输出行数（0 表示无限制）

	// extSeriesStore 为外部 SeriesStore（用于 nil shard 场景的 SID→Tags 解析）
	extSeriesStore SeriesStore

	mu sync.RWMutex
}

// NewShardIterator 创建 Shard 迭代器（带时间范围过滤）。
//
// 参数：
//   - shard:     目标 Shard
//   - startTime: 查询起始时间（包含）
//   - endTime:   查询截止时间（不包含），<=0 表示无限制
//   - maxRows:   最大输出行数（0 表示无限制）
//
// 返回：
//   - *ShardIterator: 初始化后的迭代器
//
// 初始化过程：
//
//  1. 创建 MemTable 迭代器并定位到第一条记录
//  2. 创建 SSTable MergeIterator 流式归并（块级按需读取）
//  3. 记录当前位置用于归并排序
func NewShardIterator(shard *Shard, startTime, endTime int64, maxRows int) *ShardIterator {
	return NewShardIteratorWithMemTable(shard, nil, nil, startTime, endTime, maxRows, nil, nil)
}

// NewShardIteratorWithMemTable 创建 Shard 迭代器，可指定外部 MemTable 和 SeriesStore。
// externalMT 为 nil 时使用 shard 自带的 MemTable（已废弃，不再提供）。
// extSeriesStore 用于 nil shard 场景下 SID→Tags 解析。
// filterConds 用于 ZoneMap 谓词下推块跳过（nil=不过滤）。
func NewShardIteratorWithMemTable(shard *Shard, externalMT *memtable.MemTable, extSeriesStore SeriesStore, startTime, endTime int64, maxRows int, fields []string, filterConds []sstable.FilterCondition) *ShardIterator {
	db := ""
	measurement := ""
	if shard != nil {
		db = shard.db
		measurement = shard.measurement
	}

	si := &ShardIterator{
		shard:          shard,
		db:             db,
		measurement:    measurement,
		startTime:      startTime,
		endTime:        endTime,
		maxRows:        maxRows,
		extSeriesStore: extSeriesStore,
	}

	mt := externalMT
	if mt == nil && shard != nil {
		// Shard 不再持有 MemTable，当 shard 非 nil 且未提供外部 MemTable 时跳过 MemTable 迭代
		mt = nil
	}
	// 创建 MemTable 迭代器
	if mt != nil {
		si.memIter = mt.Iterator()
		if si.memIter.Next() {
			ip := si.memIter.Point()
			si.memRow = si.pointToRow(ip)
		}
	}

	// 创建流式 SSTable MergeIterator（仅当 shard 非 nil 时）
	if si.shard != nil {
		sstFiles := si.shard.listSSTableFiles()
		if len(sstFiles) > 0 {
			schema, err := si.shard.GetSchema()
			if err != nil {
				si.err = fmt.Errorf("get schema: %w", err)
				return si
			}
			sstIter, err := sstable.NewMergeIterator(sstFiles, startTime, endTime, schema, si.shard, fields, filterConds)
			if err != nil {
				si.err = fmt.Errorf("create SSTable merge iterator: %w", err)
				return si
			}
			si.sstIter = sstIter
			if sstIter.Next() {
				si.sstRow = si.resolveTags(sstIter.Point())
			}
		}
	}

	return si
}

// pointToRow 将 InternalPoint 转换为 PointRow，通过 Sid 从 SeriesStore 恢复 Tags。
func (si *ShardIterator) pointToRow(ip types.InternalPoint) *types.PointRow {
	tags := make(map[string]string)
	if si.shard != nil && si.shard.seriesStore != nil {
		tags, _ = si.shard.seriesStore.GetTags(si.shard.db, si.shard.measurement, ip.Sid) // bool 表示是否找到，忽略则使用空 tags
	} else if si.extSeriesStore != nil {
		tags, _ = si.extSeriesStore.GetTags(si.db, si.measurement, ip.Sid) // bool 表示是否找到，忽略则使用空 tags
	}
	return &types.PointRow{
		Sid:       ip.Sid,
		Timestamp: ip.Timestamp,
		Tags:      tags,
		Fields:    types.InternalFieldsToFieldEntry(ip.Fields),
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

	// 检查是否已达到输出上限
	if si.maxRows > 0 && si.produced >= si.maxRows {
		return nil
	}

	// 循环选择并检查，直到找到符合条件的或两者都耗尽
	for {
		// 选择 timestamp 较小的数据源
		var row *types.PointRow
		if si.memRow != nil && si.sstRow != nil {
			if si.memRow.Timestamp < si.sstRow.Timestamp {
				row = si.memRow
				si.memRow = si.nextMemRowLocked()
			} else {
				row = si.sstRow
				si.sstRow = si.nextSstRowLocked()
			}
		} else if si.memRow != nil {
			row = si.memRow
			si.memRow = si.nextMemRowLocked()
		} else if si.sstRow != nil {
			row = si.sstRow
			si.sstRow = si.nextSstRowLocked()
		} else {
			// 都耗尽了
			return nil
		}

		// 检查范围
		if row.Timestamp >= si.startTime && (si.endTime <= 0 || row.Timestamp < si.endTime) {
			si.produced++
			return row
		}
		// 不在范围内，继续循环获取下一个
	}
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
	return nil
}

// nextMemRow 获取下一个 MemTable row（已持有锁）
func (si *ShardIterator) nextMemRowLocked() *types.PointRow {
	for si.memIter.Next() {
		ip := si.memIter.Point()
		return si.pointToRow(ip)
	}
	return nil
}

// nextSstRow 获取下一个 SSTable row（已持有锁）
func (si *ShardIterator) nextSstRowLocked() *types.PointRow {
	if si.sstIter == nil {
		return nil
	}
	if si.sstIter.Next() {
		return si.resolveTags(si.sstIter.Point())
	}
	return nil
}

// resolveTags 为 SSTable 返回的 PointRow 填充 Tags 字段（通过 Sid 从 SeriesStore 查询）。
func (si *ShardIterator) resolveTags(row *types.PointRow) *types.PointRow {
	if row == nil {
		return nil
	}
	if si.shard != nil && si.shard.seriesStore != nil {
		tags, _ := si.shard.seriesStore.GetTags(si.shard.db, si.shard.measurement, row.Sid) // bool 表示是否找到，忽略则使用空 tags
		row.Tags = tags
	} else if si.extSeriesStore != nil {
		tags, _ := si.extSeriesStore.GetTags(si.db, si.measurement, row.Sid) // bool 表示是否找到，忽略则使用空 tags
		row.Tags = tags
	}
	return row
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

// Close 释放 SSTable MergeIterator 持有的资源。
func (si *ShardIterator) Close() {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.sstIter != nil {
		_ = si.sstIter.Close()
		si.sstIter = nil
	}
}

// Err 返回迭代过程中发生的错误。
//
// 返回：
//   - error: 迭代错误，如果无错误返回 nil
func (si *ShardIterator) Err() error {
	return si.err
}
