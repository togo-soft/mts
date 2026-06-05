package compaction

import (
	"container/heap"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// MergeIterator k-way merge 迭代器。
type MergeIterator struct {
	iterators    []*sstable.Iterator
	heap         *MergeHeap
	current      *MergeHeapItem // 复用的堆项，避免每次 Next 分配
	currentPoint *types.PointRow
	err          error
}

// MergeHeapItem 是归并堆中的一个元素，包含迭代器引用和当前数据点。
type MergeHeapItem struct {
	Iter      *sstable.Iterator
	Point     *types.PointRow
	Idx       int
	Timestamp int64
}

// MergeHeap 是实现 container/heap.Interface 的归并最小堆，按 Timestamp 排序。
type MergeHeap []*MergeHeapItem

func (h MergeHeap) Len() int { return len(h) }

func (h MergeHeap) Less(i, j int) bool {
	if h[i].Timestamp != h[j].Timestamp {
		return h[i].Timestamp < h[j].Timestamp
	}
	return h[i].Idx < h[j].Idx
}

func (h MergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MergeHeap) Push(x any) {
	*h = append(*h, x.(*MergeHeapItem))
}

func (h *MergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// NewMergeIterator 创建归并迭代器，将多个有序 SSTable 迭代器合并为单一有序输出流。
func NewMergeIterator(iters []*sstable.Iterator) *MergeIterator {
	h := make(MergeHeap, 0, len(iters))

	for i, iter := range iters {
		if iter.Next() {
			p := iter.Point()
			h = append(h, &MergeHeapItem{
				Iter:      iter,
				Point:     p,
				Idx:       i,
				Timestamp: p.Timestamp,
			})
		}
	}

	heap.Init(&h)

	return &MergeIterator{
		iterators: iters,
		heap:      &h,
	}
}

func (m *MergeIterator) Next() bool {
	if len(*m.heap) == 0 || m.err != nil {
		m.current = nil
		m.currentPoint = nil
		return false
	}

	m.current = heap.Pop(m.heap).(*MergeHeapItem)
	m.currentPoint = m.current.Point

	if m.current.Iter.Next() {
		p := m.current.Iter.Point()
		m.current.Point = p
		m.current.Timestamp = p.Timestamp
		heap.Push(m.heap, m.current)
	}

	return true
}

func (m *MergeIterator) Point() *types.PointRow {
	if m.currentPoint == nil {
		return nil
	}
	return m.currentPoint
}

func (m *MergeIterator) Error() error {
	return m.err
}
