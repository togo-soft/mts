package query

import (
	"container/heap"
	"context"

	"micro-ts/internal/storage/shard"
	"micro-ts/types"
)

// QueryIterator 流式查询迭代器
type QueryIterator struct {
	ctx context.Context
	req *types.QueryRangeRequest

	// Min-heap 用于多 Shard 归并排序
	heap shardHeap

	// 当前行
	currentRow *types.PointRow
	consumed   int64 // 已返回的行数
	skipped    int64 // 已跳过的行数（用于 offset）
	closed     bool  // 是否已关闭
}

type shardHeap []*shard.ShardIterator

func (h shardHeap) Len() int { return len(h) }

func (h shardHeap) Less(i, j int) bool {
	iRow := h[i].Current()
	jRow := h[j].Current()
	if iRow == nil {
		return false
	}
	if jRow == nil {
		return true
	}
	return iRow.Timestamp < jRow.Timestamp
}

func (h shardHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *shardHeap) Push(x any) {
	*h = append(*h, x.(*shard.ShardIterator))
}

func (h *shardHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// NewQueryIterator 创建流式查询迭代器
func NewQueryIterator(ctx context.Context, shards []*shard.Shard, req *types.QueryRangeRequest) *QueryIterator {
	q := &QueryIterator{
		ctx: ctx,
		req: req,
	}

	// 使用请求中的原始时间（假设为纳秒）
	startTimeNs := req.StartTime
	endTimeNs := req.EndTime

	// 为每个 Shard 创建 ShardIterator 并加入 heap
	// 注意：不进行 shard boundary check，因为 ShardIterator 内部会进行时间过滤
	q.heap = make(shardHeap, 0, len(shards))
	for _, s := range shards {
		si := shard.NewShardIterator(s, startTimeNs, endTimeNs)
		if si.Current() != nil {
			q.heap = append(q.heap, si)
		}
	}
	heap.Init(&q.heap)

	// 获取第一个有效的行
	q.fetchNextValid()

	return q
}

// fetchNextValid 获取下一个有效的 row
func (q *QueryIterator) fetchNextValid() {
	q.currentRow = nil
	for len(q.heap) > 0 {
		// 弹出最小 timestamp 的 ShardIterator
		si := heap.Pop(&q.heap).(*shard.ShardIterator)
		row := si.Current()

		// 获取下一个元素用于后续归并
		next := si.Next()
		if next != nil {
			heap.Push(&q.heap, si)
		}

		// row 为 nil 表示该 Shard 已完全耗尽
		if row == nil {
			continue
		}

		// 应用 tag filter
		if q.matchTags(row) {
			q.currentRow = q.projectFields(row)
			return
		}
		// 不匹配，继续循环
	}
}

// matchTags 检查 row 是否匹配 tag 过滤条件
func (q *QueryIterator) matchTags(row *types.PointRow) bool {
	for k, v := range q.req.Tags {
		if row.Tags[k] != v {
			return false
		}
	}
	return true
}

// projectFields 对 row 进行字段投影
func (q *QueryIterator) projectFields(row *types.PointRow) *types.PointRow {
	if len(q.req.Fields) == 0 {
		return row
	}
	filtered := make(map[string]any)
	for _, name := range q.req.Fields {
		if v, ok := row.Fields[name]; ok {
			filtered[name] = v
		}
	}
	return &types.PointRow{
		SID:       row.SID,
		Timestamp: row.Timestamp,
		Tags:      row.Tags,
		Fields:    filtered,
	}
}

// Next 移动到下一个点
func (q *QueryIterator) Next() bool {
	if q.closed {
		return false
	}
	for {
		// 检查 context 取消
		select {
		case <-q.ctx.Done():
			return false
		default:
		}

		// 如果没有当前行，获取下一个
		if q.currentRow == nil {
			q.fetchNextValid()
			if q.currentRow == nil {
				return false
			}
		}

		// 应用 offset
		if q.skipped < q.req.Offset {
			q.skipped++
			q.currentRow = nil
			continue
		}
		// 应用 limit
		if q.req.Limit > 0 && q.consumed >= q.req.Limit {
			return false
		}
		q.consumed++
		return true
	}
}

// Points 返回当前点
func (q *QueryIterator) Points() *types.PointRow {
	row := q.currentRow
	// Points() 被调用后，清空 currentRow，以便 Next() 获取下一行
	q.currentRow = nil
	return row
}

// Close 关闭迭代器
func (q *QueryIterator) Close() error {
	q.closed = true
	return nil
}
