// Package query 实现查询处理和执行。
//
// QueryIterator 提供流式查询功能，支持大数据集的高效遍历。
//
// 设计模式：
//
//	遵循 Go 迭代器模式，支持 for it.Next(ctx) { row := it.Points() } 用法。
//	内部使用最小堆（min-heap）实现多 Shard 归并排序。
package query

import (
	"container/heap"
	"context"

	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// QueryIterator 是流式查询迭代器，支持多 Shard 归并排序和过滤。
//
// QueryIterator 提供按需加载数据的迭代接口，适合处理超出内存容量的大查询。
// 内部使用最小堆（min-heap）实现多 Shard 数据的归并排序。
//
// 字段说明：
//
//   - req:      查询请求
//   - heap:     用于归并排序的最小堆
//   - currentRow: 当前迭代位置的数据
//   - consumed: 已返回给调用者的行数
//   - skipped:  因 Offset 跳过的行数
//   - closed:   是否已关闭
//
// 使用模式：
//
//	it, err := query.NewQueryIterator(ctx, shards, req)
//	if err != nil {
//	    return err
//	}
//	defer it.Close()
//
//	for it.Next(ctx) {
//	    row := it.Points()
//	    // 处理 row
//	}
//
// 线程安全：
//
//	不是线程安全的，不要从多个 goroutine 并发访问。
type QueryIterator struct {
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

// NewQueryIterator 创建流式查询迭代器。
//
// 为每个可用的 Shard 创建 ShardIterator，并加入归并排序堆。
//
// 参数：
//   - ctx:    上下文
//   - shards: 要查询的 Shard 列表
//   - req:    查询请求
//
// 返回：
//
//   - *QueryIterator: 创建的迭代器
//
// 初始化过程：
//
//  1. 为每个 Shard 创建 ShardIterator
//  2. 如果 Iterator 有当前数据，加入堆
//  3. 获取第一个有效行
func NewQueryIterator(ctx context.Context, shards []*shard.Shard, req *types.QueryRangeRequest) *QueryIterator {
	q := &QueryIterator{
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

// Next 移动到下一个匹配的数据点。
//
// 参数：
//   - ctx: 上下文，用于检查取消和超时
//
// 返回：
//
//   - bool: 如果返回 true，表示有有效数据，可通过 Points() 获取。
//     如果返回 false，表示迭代结束或出错。
//
// 功能说明：
//
//   - 处理 Offset：跳过前 req.Offset 行
//   - 处理 Limit：当 consumed >= req.Limit 时停止
//   - 自动检查 context 取消
//   - 维护 consumed 和 skipped 计数
func (q *QueryIterator) Next(ctx context.Context) bool {
	if q.closed {
		return false
	}
	for {
		// 检查 context 取消
		select {
		case <-ctx.Done():
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

// Points 返回当前迭代位置的数据。
//
// 返回：
//
//   - *types.PointRow: 当前数据行
//
// 调用时机：
//
//	在 Next() 返回 true 后才能调用，否则返回 nil。
//
// 注意：
//
//	调用 Points() 后会清空 currentRow，下次调用 Next() 会获取下一行。
func (q *QueryIterator) Points() *types.PointRow {
	row := q.currentRow
	// Points() 被调用后，清空 currentRow，以便 Next() 获取下一行
	q.currentRow = nil
	return row
}

// Close 关闭迭代器。
//
// 返回：
//   - error: 关闭失败时返回错误（当前总是返回 nil）
//
// 说明：
//
//	标记迭代器为已关闭，后续 Next() 调用将返回 false。
//	建议配合 defer 使用以确保资源释放。
func (q *QueryIterator) Close() error {
	q.closed = true
	return nil
}
