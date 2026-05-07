// Package shard 实现分片存储管理。
//
// Compaction 机制用于合并多个 SSTable，去除重复数据，优化查询性能。
//
// 设计原则：
//
//   - 流式处理：内存高效，不一次性加载所有数据
//   - 写入保护：正在写入的 SSTable 不参与 compaction
//   - 互斥执行：同一时间只有一个 compaction 在执行
//   - 可中断：context 取消时优雅退出
package shard

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// ShardSizeLimit 单个 Shard 数据大小上限（字节）。
// 超过此值后，该 Shard 不再参与 compaction。
const ShardSizeLimit = 1 * 1024 * 1024 * 1024 // 1GB

// CompactionConfig Compaction 配置。
type CompactionConfig struct {
	// 最大 SSTable 数量，超过此值触发 compaction
	MaxSSTableCount int

	// 单次 compaction 最大文件数（0 表示不限制）
	MaxCompactionBatch int

	// 单个 Shard 数据大小上限（字节），超过后不参与 compaction
	ShardSizeLimit int64

	// 定时触发间隔（0 表示禁用定时触发）
	CheckInterval time.Duration

	// Compaction 超时时间
	Timeout time.Duration
}

// DefaultCompactionConfig 返回默认配置。
func DefaultCompactionConfig() *CompactionConfig {
	return &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 0,
		ShardSizeLimit:     ShardSizeLimit,
		CheckInterval:      1 * time.Hour,
		Timeout:            30 * time.Minute,
	}
}

// CompactionTask 描述一次 compaction 任务。
type CompactionTask struct {
	// 输入文件
	inputFiles []string

	// 输出文件路径
	outputPath string

	// 进度 (0-100)
	progress int

	// 开始时间
	startedAt time.Time

	// 合并后保留的记录数
	outputCount int

	// 删除的重复记录数
	duplicateCount int
}

// NewCompactionTask 创建 compaction 任务。
func NewCompactionTask(inputFiles []string, outputPath string) *CompactionTask {
	return &CompactionTask{
		inputFiles: inputFiles,
		outputPath: outputPath,
		progress:   0,
		startedAt:  time.Now(),
	}
}

// CompactionManager 管理 Shard 的 Compaction。
type CompactionManager struct {
	shard  *Shard
	config *CompactionConfig
	mu     sync.Mutex // 保护 compaction 状态

	// 定时触发相关
	ticker            *time.Ticker
	stopCh            chan struct{}
	stopOnce          sync.Once // 确保 Stop 只执行一次
	wg                sync.WaitGroup
	lastCompact       time.Time // 上次 compaction 完成时间
	compactMu         sync.Mutex
	compactInProgress int32 // atomic: 0=空闲, 1=执行中
}

// NewCompactionManager 创建 CompactionManager。
func NewCompactionManager(shard *Shard, config *CompactionConfig) *CompactionManager {
	if config == nil {
		config = DefaultCompactionConfig()
	}
	return &CompactionManager{
		shard:  shard,
		config: config,
		stopCh: make(chan struct{}),
	}
}

// Compact 执行 compaction 合并。
//
// 返回合并后的 SSTable 路径和被删除的旧文件列表。
func (cm *CompactionManager) Compact(ctx context.Context) (string, []string, error) {
	cm.mu.Lock()

	// 收集待合并的 SSTable
	sstFiles, err := cm.collectSSTables()
	if err != nil {
		cm.mu.Unlock()
		return "", nil, fmt.Errorf("collect sstables: %w", err)
	}

	if len(sstFiles) < 2 {
		cm.mu.Unlock()
		return "", nil, nil
	}

	// 如果配置了最大文件数限制
	if cm.config.MaxCompactionBatch > 0 && len(sstFiles) > cm.config.MaxCompactionBatch {
		sstFiles = sstFiles[:cm.config.MaxCompactionBatch]
	}

	// 获取下一个 SSTable 序列号
	outputSeq := cm.shard.NextSSTSeq()
	outputPath := filepath.Join(cm.shard.DataDir(), fmt.Sprintf("sst_%d", outputSeq))

	task := NewCompactionTask(sstFiles, outputPath)

	// 释放锁，允许其他操作继续
	cm.mu.Unlock()

	// 执行归并
	if err := cm.merge(ctx, task); err != nil {
		_ = os.RemoveAll(outputPath)
		return "", nil, fmt.Errorf("merge failed: %w", err)
	}

	// 重新加锁提交
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if err := cm.commit(task); err != nil {
		_ = os.RemoveAll(outputPath)
		return "", nil, fmt.Errorf("commit failed: %w", err)
	}

	return task.outputPath, task.inputFiles, nil
}

// collectSSTables 收集需要 compaction 的 SSTable。
//
// 收集规则：
//  1. 只收集已完成写入的 SSTable（排除正在写入的）
//  2. 按文件名排序（确保序号小的先处理）
func (cm *CompactionManager) collectSSTables() ([]string, error) {
	dataDir := filepath.Join(cm.shard.Dir(), "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var sstFiles []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "sst_") {
			continue
		}

		sstPath := filepath.Join(dataDir, entry.Name())

		// 检查是否正在被写入
		if cm.isSSTableInWrite(sstPath) {
			slog.Debug("skipping sstable in write state", "path", sstPath)
			continue
		}

		sstFiles = append(sstFiles, sstPath)
	}

	// 按文件名排序
	sort.Strings(sstFiles)
	return sstFiles, nil
}

// isSSTableInWrite 检查 SSTable 是否正在被写入。
func (cm *CompactionManager) isSSTableInWrite(sstPath string) bool {
	writingFlag := filepath.Join(sstPath, ".writing")
	_, err := os.Stat(writingFlag)
	return err == nil
}

// markSSTableWriting 开始写入标记。
func (cm *CompactionManager) markSSTableWriting(sstPath string) error {
	// 确保目录存在
	if err := os.MkdirAll(sstPath, 0755); err != nil {
		return err
	}
	writingFlag := filepath.Join(sstPath, ".writing")
	f, err := os.Create(writingFlag)
	if err != nil {
		return err
	}
	return f.Close()
}

// unmarkSSTableWriting 结束写入标记。
func (cm *CompactionManager) unmarkSSTableWriting(sstPath string) error {
	writingFlag := filepath.Join(sstPath, ".writing")
	return os.Remove(writingFlag)
}

// merge 执行归并操作。
func (cm *CompactionManager) merge(ctx context.Context, task *CompactionTask) error {
	// 打开所有输入文件
	readers := make([]*sstable.Reader, 0, len(task.inputFiles))
	for _, path := range task.inputFiles {
		r, err := sstable.NewReader(path)
		if err != nil {
			// 关闭已打开的 readers
			for _, r := range readers {
				_ = r.Close()
			}
			return fmt.Errorf("open sstable reader for %s: %w", path, err)
		}
		readers = append(readers, r)
	}

	// 统一关闭所有 readers
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	// 创建输出 Writer
	// 从 outputPath 解析序列号，避免重复调用 NextSSTSeq
	seqStr := filepath.Base(task.outputPath)
	var outputSeq uint64
	if _, err := fmt.Sscanf(seqStr, "sst_%d", &outputSeq); err != nil {
		return fmt.Errorf("parse output seq from path: %w", err)
	}
	w, err := sstable.NewWriter(cm.shard.Dir(), outputSeq, 0)
	if err != nil {
		return err
	}

	// 创建迭代器
	iterators := make([]*sstable.Iterator, 0, len(readers))
	for _, r := range readers {
		it, err := r.NewIterator()
		if err != nil {
			return err
		}
		iterators = append(iterators, it)
	}

	// k-way merge
	merged := newMergeIterator(iterators)

	// 用于去重：(timestamp, sid) -> 已见过的标记
	seen := make(map[string]bool)

	// 用于收集待写入的点
	var pointsToWrite []*types.Point
	var tsSidMap map[int64]uint64
	const batchSize = 1000 // 每 1000 条批量写入

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

		// 去重：相同 (timestamp, sid) 只保留一个
		if seen[key] {
			task.duplicateCount++
			continue
		}
		seen[key] = true

		// 转换为 Point 并收集
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

		// 批量写入
		if len(pointsToWrite) >= batchSize {
			if err := flushBatch(); err != nil {
				_ = w.Close()
				return err
			}
		}
	}

	if err := merged.Error(); err != nil {
		_ = w.Close()
		return err
	}

	// 写入剩余的点
	if err := flushBatch(); err != nil {
		_ = w.Close()
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	return nil
}

// commit 原子性提交 compaction 结果。
func (cm *CompactionManager) commit(task *CompactionTask) error {
	// 验证输出文件完整性
	if err := cm.verifyOutput(task.outputPath); err != nil {
		return fmt.Errorf("verify output: %w", err)
	}

	// 删除旧文件
	var lastErr error
	for _, oldFile := range task.inputFiles {
		if err := os.RemoveAll(oldFile); err != nil {
			slog.Warn("failed to remove old sstable", "path", oldFile, "error", err)
			lastErr = err
		}
	}

	// 更新 lastCompact 时间
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
	// 检查输出目录是否存在
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("output path stat: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path is not a directory")
	}

	// 检查必要的文件是否存在
	requiredFiles := []string{"_timestamps.bin", "_sids.bin", "_schema.json"}
	for _, f := range requiredFiles {
		filePath := filepath.Join(path, f)
		if _, err := os.Stat(filePath); err != nil {
			return fmt.Errorf("missing required file: %s", f)
		}
	}

	return nil
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
	// 最小堆：按 timestamp 排序
	if h[i].timestamp != h[j].timestamp {
		return h[i].timestamp < h[j].timestamp
	}
	// timestamp 相同时按 idx 排序保证确定性
	return h[i].idx < h[j].idx
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push 实现 heap.Interface。
func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*mergeHeapItem))
}

// Pop 实现 heap.Interface。
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func newMergeIterator(iters []*sstable.Iterator) *mergeIterator {
	h := make(mergeHeap, 0, len(iters))

	// 初始化：每个迭代器 advance 到第一个元素
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

	// 构建最小堆
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

	// 弹出最小元素
	m.current = heap.Pop(m.heap).(*mergeHeapItem)

	// 将该迭代器 advance 到下一个元素
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

// ShouldCompact 检查是否应该触发 compaction。
func (cm *CompactionManager) ShouldCompact() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.shouldCompactLocked()
}

// shouldCompactLocked 检查是否应该触发 compaction（已持有锁）。
func (cm *CompactionManager) shouldCompactLocked() bool {
	// 检查 1: SSTable 数量是否超限
	files, err := cm.collectSSTables()
	if err != nil {
		return false
	}

	if len(files) < cm.config.MaxSSTableCount {
		return false
	}

	// 检查 2: Shard 总大小是否超过限制
	shardSize, err := cm.calculateShardSize()
	if err != nil {
		return false
	}

	if shardSize >= cm.config.ShardSizeLimit {
		slog.Info("shard size exceeds limit, skipping compaction",
			"shard", cm.shard.Dir(),
			"size", shardSize,
			"limit", cm.config.ShardSizeLimit)
		return false
	}

	return true
}

// ShouldCompactWithLock 检查是否应该触发 compaction（调用者需持有锁）。
func (cm *CompactionManager) ShouldCompactWithLock() bool {
	return cm.shouldCompactLocked()
}

// calculateShardSize 计算 Shard 数据目录总大小。
func (cm *CompactionManager) calculateShardSize() (int64, error) {
	dataDir := filepath.Join(cm.shard.Dir(), "data")
	var totalSize int64

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "sst_") {
			continue
		}
		sstPath := filepath.Join(dataDir, entry.Name())
		size, err := dirSize(sstPath)
		if err != nil {
			continue
		}
		totalSize += size
	}

	return totalSize, nil
}

// dirSize 计算目录大小（递归）。
func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// StartPeriodicCheck 启动定期检查。
func (cm *CompactionManager) StartPeriodicCheck() {
	if cm.config.CheckInterval <= 0 {
		return
	}

	cm.ticker = time.NewTicker(cm.config.CheckInterval)
	ticker := cm.ticker // 捕获局部变量避免竞态
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		for {
			select {
			case <-ticker.C:
				cm.doPeriodicCompaction()
			case <-cm.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

// Stop 停止定期检查。
func (cm *CompactionManager) Stop() {
	cm.stopOnce.Do(func() {
		close(cm.stopCh)
	})
	cm.wg.Wait()
}

// doPeriodicCompaction 定时执行的 compaction。
func (cm *CompactionManager) doPeriodicCompaction() {
	if !cm.tryAcquireCompactLock() {
		return
	}
	defer cm.releaseCompactLock()

	ctx, cancel := context.WithTimeout(context.Background(), cm.config.Timeout)
	defer cancel()

	if !cm.shouldCompactLocked() {
		return
	}

	_, _, err := cm.Compact(ctx)
	if err != nil {
		slog.Error("periodic compaction failed", "error", err)
	}

	cm.resetTimer()
}

// tryAcquireCompactLock 尝试获取 compaction 执行锁。
func (cm *CompactionManager) tryAcquireCompactLock() bool {
	return atomic.CompareAndSwapInt32(&cm.compactInProgress, 0, 1)
}

// releaseCompactLock 释放 compaction 执行锁。
func (cm *CompactionManager) releaseCompactLock() {
	atomic.StoreInt32(&cm.compactInProgress, 0)
}

// resetTimer 重置定时器。
func (cm *CompactionManager) resetTimer() {
	cm.compactMu.Lock()
	cm.lastCompact = time.Now()
	cm.compactMu.Unlock()

	if cm.ticker != nil {
		cm.ticker.Reset(cm.config.CheckInterval)
	}
}
