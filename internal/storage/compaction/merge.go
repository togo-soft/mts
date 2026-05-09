package compaction

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

// Merge 执行归并操作。
func (cm *CompactionManager) Merge(ctx context.Context, task *CompactionTask) error {
	schema, err := cm.ShardAccess.GetSchema()
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}

	readers := make([]*sstable.Reader, 0, len(task.InputFiles))
	for _, path := range task.InputFiles {
		r, err := sstable.NewReader(path, schema)
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

	tombstones := collectInputTombstones(task.InputFiles)

	seqStr := filepath.Base(task.OutputPath)
	var outputSeq uint64
	if _, err := fmt.Sscanf(seqStr, "sst_%d", &outputSeq); err != nil {
		return fmt.Errorf("parse output seq from path: %w", err)
	}
	w, err := sstable.NewWriter(cm.ShardAccess.Dir(), outputSeq, 0)
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

	merged := NewMergeIterator(iterators)

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
		task.OutputCount += len(pointsToWrite)
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
			task.DuplicateCount++
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
			cm.ReportProgress(task.OutputCount)
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

	return SaveTombstones(task.OutputPath, tombstones)
}

// commit 原子性提交 compaction 结果。
func (cm *CompactionManager) Commit(task *CompactionTask) error {
	if err := cm.VerifyOutput(task.OutputPath); err != nil {
		return fmt.Errorf("verify output: %w", err)
	}

	var lastErr error
	for _, oldFile := range task.InputFiles {
		if !cm.ShardAccess.IsSSTUnused(oldFile) {
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

func (cm *CompactionManager) VerifyOutput(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("output path stat: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path is not a directory")
	}

	requiredFiles := []string{"_timestamps.bin", "_sids.bin"}
	for _, f := range requiredFiles {
		filePath := filepath.Join(path, f)
		if _, err := os.Stat(filePath); err != nil {
			return fmt.Errorf("missing required file: %s", f)
		}
	}

	return nil
}

func (cm *CompactionManager) ReportProgress(outputCount int) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	if cm.CurrentTask == nil {
		return
	}
	cm.CurrentTask.Progress = outputCount
}

// collectInputTombstones 收集所有输入 SSTable 的删除标记，合并为一个集合。
func collectInputTombstones(inputPaths []string) *TombstoneSet {
	var all []Tombstone
	for _, path := range inputPaths {
		ts, err := LoadTombstones(path)
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

// MergeIterator k-way merge 迭代器。
type MergeIterator struct {
	iterators []*sstable.Iterator
	heap      *MergeHeap
	current   *MergeHeapItem
	err       error
}

type MergeHeapItem struct {
	Iter      *sstable.Iterator
	Point     *types.PointRow
	Idx       int
	Timestamp int64
}

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
		return false
	}

	m.current = heap.Pop(m.heap).(*MergeHeapItem)

	if m.current.Iter.Next() {
		p := m.current.Iter.Point()
		m.current.Point = p
		m.current.Timestamp = p.Timestamp
		heap.Push(m.heap, m.current)
	}

	return true
}

func (m *MergeIterator) Point() *types.PointRow {
	if m.current == nil {
		return nil
	}
	return m.current.Point
}

func (m *MergeIterator) Error() error {
	return m.err
}
