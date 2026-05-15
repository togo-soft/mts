package engine

import (
	"context"
	"fmt"

	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// Iterator 返回流式查询迭代器。
// 合并 Writer MemTable（未刷盘数据）和 Shard SSTable（已刷盘数据）。
func (e *Engine) Iterator(ctx context.Context, req *types.QueryRangeRequest) (*query.Iterator, error) {
	if e.isClosed() {
		return nil, fmt.Errorf("engine is closed")
	}

	// 获取已存在的 writer（查询不创建新的）
	var writerMT *memtable.MemTable
	var extSeriesStore shard.SeriesStore
	if w := e.shardManager.GetWriterIfExists(req.Database, req.Measurement); w != nil {
		writerMT = w.MemTable()
		extSeriesStore = w.SeriesStore()
	}

	shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)
	if len(shards) == 0 && writerMT == nil {
		return nil, fmt.Errorf("no shards found")
	}

	return query.NewIteratorWithMemTable(ctx, shards, writerMT, extSeriesStore, req), nil
}

// IteratorWithMemTable 是包内使用的辅助函数（供测试等场景使用）。
func IteratorWithMemTable(ctx context.Context, shards []*shard.Shard, wmt *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest) *query.Iterator {
	return query.NewIteratorWithMemTable(ctx, shards, wmt, extSeriesStore, req)
}
