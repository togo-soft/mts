// Package query 实现查询处理和执行。
//
// Iterator 提供流式查询功能，支持大数据集的高效遍历。
//
// 设计模式：
//
//	遵循 Go 迭代器模式，支持 for it.Next(ctx) { row := it.Points() } 用法。
//	内部使用最小堆（min-heap）实现多 Shard 归并排序。
package query

import (
	"container/heap"
	"context"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// Iterator 是流式查询迭代器，支持多 Shard 归并排序和过滤。
//
// Iterator 提供按需加载数据的迭代接口，适合处理超出内存容量的大查询。
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
//	it, err := query.NewIterator(ctx, shards, req)
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
type Iterator struct {
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

// NewIterator 创建流式查询迭代器。
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
//   - *Iterator: 创建的迭代器
//
// 初始化过程：
//
//  1. 为每个 Shard 创建 ShardIterator
//  2. 如果 Iterator 有当前数据，加入堆
//  3. 获取第一个有效行
func NewIterator(ctx context.Context, shards []*shard.Shard, req *types.QueryRangeRequest) *Iterator {
	q := &Iterator{
		req: req,
	}

	// 使用请求中的原始时间（假设为纳秒）
	startTimeNs := req.StartTime
	endTimeNs := req.EndTime

	// 为每个 Shard 创建 ShardIterator 并加入 heap
	// 注意：不进行 shard boundary check，因为 ShardIterator 内部会进行时间过滤
	// maxRows: 每个 shard 最多需要提供 req.Limit+req.Offset 行（最坏情况所有行来自同一 shard）
	var maxRows int
	if req.Limit > 0 {
		maxRows = int(req.Limit + req.Offset)
	}
	q.heap = make(shardHeap, 0, len(shards))
	for _, s := range shards {
		si := shard.NewShardIterator(s, startTimeNs, endTimeNs, maxRows)
		if si.Current() != nil {
			q.heap = append(q.heap, si)
		} else {
			// 该 Shard 无匹配数据，释放其持有的 SSTable 引用
			si.Close()
		}
	}
	heap.Init(&q.heap)

	// 获取第一个有效的行
	q.fetchNextValid()

	return q
}

// NewIteratorWithMemTable 创建流式查询迭代器，合并 Writer MemTable 和 Shard SSTable。
// writerMT 为 MeasurementWriter 的 MemTable（未刷盘数据），可为 nil。
// extSeriesStore 用于 nil shard 场景下 SID→Tags 解析（可为 nil）。
func NewIteratorWithMemTable(ctx context.Context, shards []*shard.Shard, writerMT *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest) *Iterator {
	q := &Iterator{
		req: req,
	}

	startTimeNs := req.StartTime
	endTimeNs := req.EndTime

	var maxRows int
	if req.Limit > 0 {
		maxRows = int(req.Limit + req.Offset)
	}
	q.heap = make(shardHeap, 0, len(shards)+1)

	for i, s := range shards {
		var si *shard.ShardIterator
		if i == 0 && writerMT != nil {
			si = shard.NewShardIteratorWithMemTable(s, writerMT, extSeriesStore, startTimeNs, endTimeNs, maxRows)
		} else {
			si = shard.NewShardIterator(s, startTimeNs, endTimeNs, maxRows)
		}
		if si.Current() != nil {
			q.heap = append(q.heap, si)
		} else {
			si.Close()
		}
	}

	// 如果没有 shard 但有 writer MemTable，创建独立数据源
	if len(shards) == 0 && writerMT != nil {
		si := shard.NewShardIteratorWithMemTable(nil, writerMT, extSeriesStore, startTimeNs, endTimeNs, maxRows)
		if si.Current() != nil {
			q.heap = append(q.heap, si)
		} else {
			si.Close()
		}
	}

	heap.Init(&q.heap)

	q.fetchNextValid()

	return q
}

// fetchNextValid 获取下一个有效的 row
func (q *Iterator) fetchNextValid() {
	q.currentRow = nil
	for len(q.heap) > 0 {
		// 弹出最小 timestamp 的 ShardIterator
		si := heap.Pop(&q.heap).(*shard.ShardIterator)
		row := si.Current()

		// 获取下一个元素用于后续归并
		next := si.Next()
		if next != nil {
			heap.Push(&q.heap, si)
		} else {
			// Shard 已耗尽，释放其持有的 SSTable 引用
			si.Close()
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
func (q *Iterator) matchTags(row *types.PointRow) bool {
	for k, v := range q.req.Tags {
		if row.Tags[k] != v {
			return false
		}
	}
	return true
}

// projectFields 对 row 进行字段投影
func (q *Iterator) projectFields(row *types.PointRow) *types.PointRow {
	if len(q.req.Fields) == 0 {
		return row
	}
	// 构建请求字段名集合用于快速匹配
	fieldSet := make(map[string]bool, len(q.req.Fields))
	for _, name := range q.req.Fields {
		fieldSet[name] = true
	}
	filtered := make([]*types.FieldEntry, 0, len(q.req.Fields))
	for _, f := range row.Fields {
		if fieldSet[f.Key] {
			filtered = append(filtered, f)
		}
	}
	return &types.PointRow{
		Sid:       row.Sid,
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
func (q *Iterator) Next(ctx context.Context) bool {
	if q.closed {
		return false
	}
	// 若调用方未调用 Points() 消费上一行数据，自动跳过
	if q.consumed > 0 && q.currentRow != nil {
		q.currentRow = nil
	}
	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		if q.currentRow == nil {
			q.fetchNextValid()
			if q.currentRow == nil {
				return false
			}
		}

		if q.skipped < q.req.Offset {
			q.skipped++
			q.currentRow = nil
			continue
		}
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
func (q *Iterator) Points() *types.PointRow {
	row := q.currentRow
	// Points() 被调用后，清空 currentRow，以便 Next() 获取下一行
	q.currentRow = nil
	return row
}

// Close 关闭迭代器，释放底层 ShardIterator 持有的 SSTable 引用。
//
// 返回：
//   - error: 关闭失败时返回错误（当前总是返回 nil）
//
// 说明：
//
//	标记迭代器为已关闭，关闭所有 ShardIterator 以释放 SSTable 引用计数，
//	防止 Compaction 因引用未释放而无法清理旧文件。
//	建议配合 defer 使用以确保资源释放。
func (q *Iterator) Close() error {
	q.closed = true
	for _, si := range q.heap {
		si.Close()
	}
	return nil
}
