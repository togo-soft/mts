package sstable

import (
	"container/heap"

	"codeberg.org/micro-ts/mts/types"
)

// SSTableRefManager 管理 SSTable 文件的引用计数。
// 由 Shard 实现，用于防止 Compaction 删除正在读取的文件。
type SSTableRefManager interface {
	AcquireSSTRef(path string) bool
	ReleaseSSTRef(path string)
}

// mergeItem 是堆中的一个条目。
type mergeItem struct {
	iter  *Iterator
	point *types.PointRow
	idx   int
}

// mergeHeap 实现 container/heap.Interface，按时间戳升序。
type mergeHeap []*mergeItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	ti := h[i].point.Timestamp
	tj := h[j].point.Timestamp
	if ti == tj {
		return h[i].idx < h[j].idx
	}
	return ti < tj
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

func (h *mergeHeap) Push(x any) {
	item := x.(*mergeItem)
	item.idx = len(*h)
	*h = append(*h, item)
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}

// MergeIterator 合并多个 SSTable 文件，按时间戳升序输出。
// 内部对每个文件使用 Iterator 进行块级流式读取。
type MergeIterator struct {
	heap      mergeHeap
	current   *types.PointRow
	readers   []*Reader
	endTime   int64
	refMgr    SSTableRefManager
	openFiles []string
}

// NewMergeIterator 创建多 SSTable 合并迭代器。
//
// 参数：
//   - filePaths: SSTable 文件路径列表
//   - startTime: 查询起始时间（包含）
//   - endTime:   查询结束时间（不包含），<=0 表示无限制
//   - schema:    解码所需的 Schema
//   - refMgr:    引用计数管理器（Shard 实现），可为 nil
//   - fields:    需要投影的字段（nil=全部字段）
func NewMergeIterator(filePaths []string, startTime, endTime int64, schema Schema, refMgr SSTableRefManager, fields []string, filterConds []FilterCondition) (*MergeIterator, error) {
	mi := &MergeIterator{
		heap:      make(mergeHeap, 0, len(filePaths)),
		endTime:   endTime,
		refMgr:    refMgr,
		openFiles: make([]string, 0, len(filePaths)),
	}

	for _, fp := range filePaths {
		if refMgr != nil {
			if !refMgr.AcquireSSTRef(fp) {
				continue
			}
		}

		r, err := NewReader(fp, schema)
		if err != nil {
			if refMgr != nil {
				refMgr.ReleaseSSTRef(fp)
			}
			continue
		}
		mi.readers = append(mi.readers, r)
		mi.openFiles = append(mi.openFiles, fp)

		iter, err := r.NewIterator(fields, filterConds)
		if err != nil {
			if refMgr != nil {
				refMgr.ReleaseSSTRef(fp)
			}
			continue
		}

		if startTime > 0 {
			_ = iter.SeekToTime(startTime)
		}

		if iter.Next() {
			row := iter.Point()
			if row == nil {
				continue
			}
			if endTime > 0 && row.Timestamp >= endTime {
				continue
			}
			item := &mergeItem{iter: iter, point: row}
			heap.Push(&mi.heap, item)
		}
	}

	return mi, nil
}

// Next 移动到下一个时间戳最小的行。
func (mi *MergeIterator) Next() bool {
	if len(mi.heap) == 0 {
		return false
	}

	item := heap.Pop(&mi.heap).(*mergeItem)
	mi.current = item.point

	for item.iter.Next() {
		row := item.iter.Point()
		if row == nil {
			continue
		}
		if mi.endTime > 0 && row.Timestamp >= mi.endTime {
			continue
		}
		item.point = row
		heap.Push(&mi.heap, item)
		return true
	}

	return true
}

// Point 返回当前行。
func (mi *MergeIterator) Point() *types.PointRow {
	return mi.current
}

// Close 关闭所有底层 Reader 并释放引用。
func (mi *MergeIterator) Close() error {
	for _, r := range mi.readers {
		_ = r.Close()
	}
	if mi.refMgr != nil {
		for _, fp := range mi.openFiles {
			mi.refMgr.ReleaseSSTRef(fp)
		}
	}
	mi.readers = nil
	mi.openFiles = nil
	mi.heap = nil
	return nil
}
