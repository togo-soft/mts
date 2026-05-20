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

const (
	mergeBatchSize = 1000               // 合并写入批量刷盘大小
	hashSeed       = 0x9e3779b97f4a7c15 // 去重哈希黄金比例常量
)

// Merge 执行归并操作。
func (cm *Manager) Merge(ctx context.Context, task *Task) error {
	schema, err := cm.ShardAccess.GetSchema()
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}

	// 对所有输入文件加引用，防止在 Merge 期间被 Commit 删除
	acquiredPaths := make([]string, 0, len(task.InputFiles))
	for _, path := range task.InputFiles {
		if !cm.ShardAccess.AcquireSSTRef(path) {
			slog.Warn("failed to acquire sst ref during merge, skipping", "path", path)
			continue
		}
		acquiredPaths = append(acquiredPaths, path)
	}

	// 所有路径加完引用后再检查完整性，确保检查和 Commit 删除之间不会漏
	// 注意：skipped paths 的引用会保留到函数结束，不会被提前释放
	defer func() {
		for _, path := range acquiredPaths {
			cm.ShardAccess.ReleaseSSTRef(path)
		}
	}()

	readers := make([]*sstable.Reader, 0, len(acquiredPaths))
	mergedPaths := make([]string, 0, len(acquiredPaths))
	for _, path := range acquiredPaths {
		// 再次验证 SSTable 完整性，防止在 CollectSSTables 和 Merge 之间文件被删除或变得不完整
		if !cm.isSSTableComplete(path) {
			slog.Warn("sstable became incomplete during merge, skipping", "path", path)
			continue
		}
		// 使用 canOpenSSTable 进行二次验证
		if !cm.canOpenSSTable(path) {
			slog.Warn("sstable cannot be opened during merge, skipping", "path", path)
			continue
		}
		r, err := sstable.NewReader(path, schema)
		if err != nil {
			slog.Warn("failed to open sstable reader during merge, skipping", "path", path, "error", err)
			continue
		}
		readers = append(readers, r)
		mergedPaths = append(mergedPaths, path)
	}

	// 如果没有有效的 reader，直接返回
	if len(readers) == 0 {
		slog.Warn("no valid sstables to merge")
		return nil
	}
	if len(readers) < len(acquiredPaths) {
		slog.Info("some sstables were skipped during merge", "expected", len(acquiredPaths), "actual", len(readers))
	}

	// 记录实际被合并的文件，Commit 只删除这些文件
	task.MergedFiles = mergedPaths

	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	tombstones := collectInputTombstones(task.InputFiles)
	tombstones.BuildIndex()

	seqStr := filepath.Base(task.OutputPath)
	var outputSeq uint64
	if _, err := fmt.Sscanf(seqStr, "sst_%d", &outputSeq); err != nil {
		return fmt.Errorf("parse output seq from path: %w", err)
	}
	w, err := sstable.NewWriter(cm.ShardAccess.Dir(), outputSeq, 0, cm.ShardAccess.CompressionAlgorithm(), sstable.FlagSorted)
	if err != nil {
		return err
	}

	iterators := make([]*sstable.Iterator, 0, len(readers))
	for _, r := range readers {
		it, err := r.NewIterator(nil, nil)
		if err != nil {
			return err
		}
		iterators = append(iterators, it)
	}

	merged := NewMergeIterator(iterators)

	dedup := NewDedupFilter(0)
	pointsToWrite := make([]*types.PointRow, 0, mergeBatchSize+mergeBatchSize/10)

	flushBatch := func() error {
		if len(pointsToWrite) == 0 {
			return nil
		}
		if err := w.WritePointRows(pointsToWrite); err != nil {
			return err
		}
		task.OutputCount += len(pointsToWrite)
		pointsToWrite = pointsToWrite[:0]
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
		key := uint64(row.Timestamp) ^ (row.Sid * hashSeed)

		if dedup.Seen(key) {
			task.DuplicateCount++
			continue
		}
		if tombstones.ShouldDelete(row.Sid, row.Timestamp) {
			continue
		}

		pointsToWrite = append(pointsToWrite, row)
		if len(pointsToWrite) >= mergeBatchSize {
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

// retryDelete 带重试的文件删除，处理 Windows 下文件被短暂锁定的场景（杀软、搜索索引等）。
func retryDelete(path string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*attempt) * 50 * time.Millisecond)
		}
		// 文件可能已被其他 compaction 删除
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return nil
		}
		if err := os.Remove(path); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

// commit 原子性提交 compaction 结果。
func (cm *Manager) Commit(task *Task) error {
	if err := cm.VerifyOutput(task.OutputPath); err != nil {
		return fmt.Errorf("verify output: %w", err)
	}

	// 只删除实际被合并的文件，防止删除未参与合并的 SSTable
	filesToDelete := task.MergedFiles
	if filesToDelete == nil {
		filesToDelete = task.InputFiles
	}

	for _, oldFile := range filesToDelete {
		if !cm.ShardAccess.IsSSTUnused(oldFile) {
			slog.Warn("sstable still in use, deferring cleanup", "path", oldFile)
			continue
		}
		if err := retryDelete(oldFile); err != nil {
			// 删除失败不阻塞 Commit：输出文件是有效的，旧文件残留可被后续 compaction 清理
			slog.Warn("failed to remove old sstable after retries, deferring cleanup", "path", oldFile, "error", err)
		}
		// 清理关联的 tombstones 文件
		tombstonePath := oldFile + ".tombstones"
		if _, err := os.Stat(tombstonePath); err == nil {
			_ = os.Remove(tombstonePath)
		}
	}

	cm.compactMu.Lock()
	cm.lastCompact = time.Now()
	cm.compactMu.Unlock()

	return nil
}

func (cm *Manager) VerifyOutput(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("output path stat: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("output path is a directory, expected file")
	}
	return nil
}

func (cm *Manager) ReportProgress(outputCount int) {
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
			slog.Warn("failed to load tombstones, skipping", "path", path, "error", err)
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
	iterators    []*sstable.Iterator
	heap         *MergeHeap
	current      *MergeHeapItem // 复用的堆项，避免每次 Next 分配
	currentPoint *types.PointRow
	err          error
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
		m.currentPoint = nil
		return false
	}

	m.current = heap.Pop(m.heap).(*MergeHeapItem)
	m.currentPoint = m.current.Point // 缓存当前 Point，复用 current 后不会被覆盖

	if m.current.Iter.Next() {
		p := m.current.Iter.Point()
		// 复用已弹出 item 的结构体，避免每次 Next 分配新 MergeHeapItem
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
