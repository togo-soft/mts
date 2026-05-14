package engine

import (
	"context"
	"fmt"

	"codeberg.org/micro-ts/mts/internal/query"
	"codeberg.org/micro-ts/mts/types"
)

// Iterator 返回流式查询迭代器。
func (e *Engine) Iterator(ctx context.Context, req *types.QueryRangeRequest) (*query.Iterator, error) {
	if e.isClosed() {
		return nil, fmt.Errorf("engine is closed")
	}

	shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards found")
	}

	return query.NewIterator(ctx, shards, req), nil
}
