package shard

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// merge 执行归并操作。
func (cm *CompactionManager) merge(ctx context.Context, task *CompactionTask) error {
	readers := make([]*sstable.Reader, 0, len(task.inputFiles))
	for _, path := range task.inputFiles {
		r, err := sstable.NewReader(path)
		if err != nil {
			for _, r := range readers {
				_ = r.Close()
			}
			return fmt.Errorf("open sstable reader for %s: %w", path, err)
		}
		readers = append(readers, r)
	}

	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	tombstones := collectInputTombstones(task.inputFiles)

	seqStr := filepath.Base(task.outputPath)
	var outputSeq uint64
	if _, err := fmt.Sscanf(seqStr, "sst_%d", &outputSeq); err != nil {
		return fmt.Errorf("parse output seq from path: %w", err)
	}
	w, err := sstable.NewWriter(cm.shard.Dir(), outputSeq, 0)
	if err != nil {
		return err
	}

	iterators := make([]*sstable.Iterator, 0, len(readers))
	for _, r := range readers {
		it, err := r.NewIterator()
		if err != nil {
			return err
		}
		iterators = append(iterators, it)
	}

	merged := newMergeIterator(iterators)

	seen := make(map[string]bool)
	var pointsToWrite []*types.Point
	var tsSidMap map[int64]uint64
	const batchSize = 1000

	flushBatch := func() error {
		if len(pointsToWrite) == 0 {
			return nil
		}
		if err := w.WritePoints(pointsToWrite, tsSidMap); err != nil {
			return err
		}
		task.outputCount += len(pointsToWrite)
		pointsToWrite = pointsToWrite[:0]
		tsSidMap = make(map[int64]uint64)
		return nil
	}

	for merged.Next() {
		select {
		case <-ctx.Done():
			_ = w.Close()
			return ctx.Err()
		default:
		}

		row := merged.Point()
		key := fmt.Sprintf("%d-%d", row.Timestamp, row.Sid)

		if seen[key] {
			task.duplicateCount++
			continue
		}
		if tombstones.ShouldDelete(row.Sid, row.Timestamp) {
			continue
		}
		seen[key] = true

		point := &types.Point{
			Timestamp: row.Timestamp,
			Tags:      row.Tags,
			Fields:    row.Fields,
		}
		pointsToWrite = append(pointsToWrite, point)
		if tsSidMap == nil {
			tsSidMap = make(map[int64]uint64)
		}
		tsSidMap[row.Timestamp] = row.Sid

		if len(pointsToWrite) >= batchSize {
			if err := flushBatch(); err != nil {
				_ = w.Close()
				return err
			}
			cm.reportProgress(task.outputCount)
		}
	}

	if err := merged.Error(); err != nil {
		_ = w.Close()
		return err
	}

	if err := flushBatch(); err != nil {
		_ = w.Close()
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	return saveTombstones(task.outputPath, tombstones)
}

// commit 原子性提交 compaction 结果。
func (cm *CompactionManager) commit(task *CompactionTask) error {
	if err := cm.verifyOutput(task.outputPath); err != nil {
		return fmt.Errorf("verify output: %w", err)
	}

	var lastErr error
	for _, oldFile := range task.inputFiles {
		if !cm.shard.IsSSTUnused(oldFile) {
			slog.Warn("sstable still in use, deferring cleanup", "path", oldFile)
			continue
		}
		if err := os.RemoveAll(oldFile); err != nil {
			slog.Warn("failed to remove old sstable", "path", oldFile, "error", err)
			lastErr = err
		}
	}

	cm.compactMu.Lock()
	cm.lastCompact = time.Now()
	cm.compactMu.Unlock()

	if lastErr != nil {
		return fmt.Errorf("remove old sstable files: %w", lastErr)
	}
	return nil
}

// verifyOutput 验证输出文件完整性。
func (cm *CompactionManager) verifyOutput(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("output path stat: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path is not a directory")
	}

	requiredFiles := []string{"_timestamps.bin", "_sids.bin", "_schema.json"}
	for _, f := range requiredFiles {
		filePath := filepath.Join(path, f)
		if _, err := os.Stat(filePath); err != nil {
			return fmt.Errorf("missing required file: %s", f)
		}
	}

	return nil
}

// reportProgress 更新当前 compaction 任务的进度。
func (cm *CompactionManager) reportProgress(outputCount int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.currentTask == nil {
		return
	}
	cm.currentTask.Progress = outputCount
}

// collectInputTombstones 收集所有输入 SSTable 的删除标记，合并为一个集合。
func collectInputTombstones(inputPaths []string) *TombstoneSet {
	var all []Tombstone
	for _, path := range inputPaths {
		ts, err := loadTombstones(path)
		if err != nil {
			continue
		}
		if ts.HasTombstones() {
			all = append(all, ts.Tombstones...)
		}
	}
	if len(all) == 0 {
		return &TombstoneSet{}
	}
	return &TombstoneSet{Tombstones: all}
}

// mergeIterator k-way merge 迭代器。
type mergeIterator struct {
	iterators []*sstable.Iterator
	heap      *mergeHeap
	current   *mergeHeapItem
	err       error
}

// mergeHeapItem 最小堆中的元素。
type mergeHeapItem struct {
	iter      *sstable.Iterator
	point     *types.PointRow
	idx       int
	timestamp int64
}

// mergeHeap 实现 heap.Interface。
type mergeHeap []*mergeHeapItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	if h[i].timestamp != h[j].timestamp {
		return h[i].timestamp < h[j].timestamp
	}
	return h[i].idx < h[j].idx
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*mergeHeapItem))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func newMergeIterator(iters []*sstable.Iterator) *mergeIterator {
	h := make(mergeHeap, 0, len(iters))

	for i, iter := range iters {
		if iter.Next() {
			p := iter.Point()
			h = append(h, &mergeHeapItem{
				iter:      iter,
				point:     p,
				idx:       i,
				timestamp: p.Timestamp,
			})
		}
	}

	heap.Init(&h)

	return &mergeIterator{
		iterators: iters,
		heap:      &h,
	}
}

func (m *mergeIterator) Next() bool {
	if len(*m.heap) == 0 || m.err != nil {
		m.current = nil
		return false
	}

	m.current = heap.Pop(m.heap).(*mergeHeapItem)

	if m.current.iter.Next() {
		p := m.current.iter.Point()
		m.current.point = p
		m.current.timestamp = p.Timestamp
		heap.Push(m.heap, m.current)
	}

	return true
}

func (m *mergeIterator) Point() *types.PointRow {
	if m.current == nil {
		return nil
	}
	return m.current.point
}

func (m *mergeIterator) Error() error {
	return m.err
}
