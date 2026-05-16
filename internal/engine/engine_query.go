package engine

import (
	"context"
	"fmt"

	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// scopedSeriesStore 将 engine.SeriesStore 绑定到指定 db/meas，
// 满足 shard.SeriesStore 接口，用于 Query Iterator 中 nil shard 场景的 SID→Tags 解析。
type scopedSeriesStore struct {
	inner SeriesStore
	db    string
	meas  string
}

func (s *scopedSeriesStore) AllocateSID(database, measurement string, tags map[string]string) (uint64, error) {
	return s.inner.AllocateSID(database, measurement, tags)
}

func (s *scopedSeriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
	// 在 nil shard 场景下，shard iterator 传入空 db/meas
	if database == "" && measurement == "" {
		return s.inner.GetTags(s.db, s.meas, sid)
	}
	return s.inner.GetTags(database, measurement, sid)
}

// Iterator 返回流式查询迭代器。
// 合并全局 MemTable（未刷盘数据）和 Shard SSTable（已刷盘数据）。
func (e *Engine) Iterator(ctx context.Context, req *types.QueryRangeRequest) (*query.Iterator, error) {
	if e.isClosed() {
		return nil, fmt.Errorf("engine is closed")
	}

	// 全局 MemTable 包含所有未刷盘数据
	writerMT := e.memTable

	shards := e.flusher.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)
	if len(shards) == 0 && (writerMT == nil || writerMT.Count() == 0) {
		return nil, fmt.Errorf("no shards found")
	}

	// 创建 scoped SeriesStore，确保 nil shard 场景下能正确解析 SID→Tags
	scoped := &scopedSeriesStore{
		inner: e.seriesStore,
		db:    req.Database,
		meas:  req.Measurement,
	}

	return query.NewIteratorWithMemTable(ctx, shards, writerMT, scoped, req), nil
}

// IteratorWithMemTable 是包内使用的辅助函数（供测试等场景使用）。
func IteratorWithMemTable(ctx context.Context, shards []*shard.Shard, wmt *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest) *query.Iterator {
	return query.NewIteratorWithMemTable(ctx, shards, wmt, extSeriesStore, req)
}
