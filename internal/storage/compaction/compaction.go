// Package compaction 实现 Level Compaction 策略。
//
// Compaction 是 LSM 存储引擎的核心维护操作，通过合并旧 SSTable 减少读放大、
// 控制 SSTable 数量并清理已删除或过期数据。
package compaction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

const (
	// ShardSizeLimit 单个 Shard 最大磁盘占用，超过此限制会触发保护性拒绝写入。
	ShardSizeLimit    = 1 * 1024 * 1024 * 1024 // 1GB
	twoPhaseThreshold = 10                     // 输入 SSTable 超过此数时启用两阶段合并
)

// CompactionConfig 配置别名，复用 proto 定义的类型。
type Config = types.CompactionConfig

// DefaultConfig 返回默认配置。
func DefaultConfig() *Config {
	return &Config{
		MaxSstableCount:    4,
		MaxCompactionBatch: 0,
		ShardSizeLimit:     ShardSizeLimit,
		CheckIntervalNanos: int64(time.Hour),
		TimeoutNanos:       int64(30 * time.Minute),
	}
}

// Task 描述一次 compaction 任务。
type Task struct {
	InputFiles     []string
	MergedFiles    []string // 实际被合并的文件列表（仅这些文件可在 Commit 时安全删除）
	OutputPath     string
	Progress       int
	StartedAt      time.Time
	OutputCount    int
	DuplicateCount int
}

// NewTask 创建 compaction 任务。
func NewTask(inputFiles []string, outputPath string) *Task {
	return &Task{
		InputFiles: inputFiles,
		OutputPath: outputPath,
		Progress:   0,
		StartedAt:  time.Now(),
	}
}

// Progress 描述 compaction 进度。
type Progress struct {
	InputFiles []string
	OutputFile string
	Progress   int // 0-100
	Status     string
	StartedAt  time.Time
	Err        error
}

// Manager 管理 Shard 的 Compaction。
type Manager struct {
	ShardAccess ShardAccess
	Config      *Config
	Mu          sync.Mutex

	Ticker            *time.Ticker
	stopCh            chan struct{}
	stopOnce          sync.Once
	wg                sync.WaitGroup
	lastCompact       time.Time
	compactMu         sync.Mutex
	compactInProgress atomic.Int32
	CurrentTask       *Progress

	ctx    context.Context
	cancel context.CancelFunc
}

// NewManager 创建 Manager。
func NewManager(shard ShardAccess, config *Config) *Manager {
	if config == nil {
		config = DefaultConfig()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		ShardAccess: shard,
		Config:      config,
		stopCh:      make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Timeout 返回 compaction 超时配置。
func (cm *Manager) Timeout() time.Duration {
	return time.Duration(cm.Config.TimeoutNanos)
}

// Context 返回 manager 的可取消 context，Stop() 时会取消。
// 调用方应使用此 context 创建子 context，以便 Stop() 能打断运行中的 compaction。
func (cm *Manager) Context() context.Context {
	return cm.ctx
}

// SetConfig 运行时更新 Compaction 配置。
// 更新后自动重置 ticker 以使用新的 CheckInterval。
func (cm *Manager) SetConfig(config *Config) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	if config == nil {
		return
	}

	cm.Config = config

	if cm.Ticker != nil && config.CheckIntervalNanos > 0 {
		cm.Ticker.Reset(time.Duration(config.CheckIntervalNanos))
	}
}

// Compact 执行 compaction 合并。
func (cm *Manager) Compact(ctx context.Context) (string, []string, error) {
	// 先尝试获取 compaction 锁，防止并发 compaction
	if !cm.TryAcquireCompactLock() {
		return "", nil, nil
	}
	defer cm.ReleaseCompactLock()

	cm.Mu.Lock()
	// CollectSSTables acquires refs for all collected files to prevent deletion
	sstFiles, err := cm.CollectSSTables()
	if err != nil {
		cm.Mu.Unlock()
		return "", nil, fmt.Errorf("collect sstables: %w", err)
	}

	if len(sstFiles) < 2 {
		cm.Mu.Unlock()
		// Release refs for early return
		cm.ReleaseSSTRefs(sstFiles)
		return "", nil, nil
	}

	batchLimit := int(cm.Config.MaxCompactionBatch)
	if batchLimit <= 0 {
		batchLimit = twoPhaseThreshold
	}
	if len(sstFiles) > batchLimit {
		// 释放被截断部分的引用，防止引用泄漏
		cm.ReleaseSSTRefs(sstFiles[batchLimit:])
		sstFiles = sstFiles[:batchLimit]
	}

	outputSeq := cm.ShardAccess.NextSSTSeq()
	outputPath := filepath.Join(cm.ShardAccess.DataDir(), fmt.Sprintf("sst_%d.bin", outputSeq))

	cm.Mu.Unlock()

	// 在输出文件上添加 .writing 标记，防止并发查询读到写入中的文件
	_ = cm.MarkWriting(outputPath)

	// 两阶段合并：当文件数超过阈值时分批合并为中间文件，再合并中间文件。
	mergedFiles, mergeErr := cm.compactWithTwoPhase(ctx, sstFiles, outputPath)
	if mergeErr != nil {
		_ = cm.UnmarkWriting(outputPath)
		_ = os.Remove(outputPath) // 清理可能已部分写入的输出文件
		metrics.Incr(metrics.CompactionErrors, 1)
		cm.Mu.Lock()
		cm.CurrentTask = &Progress{Status: "failed", Err: mergeErr}
		cm.Mu.Unlock()
		cm.ReleaseSSTRefs(sstFiles)
		return "", nil, fmt.Errorf("merge failed: %w", mergeErr)
	}

	cm.ReleaseSSTRefs(sstFiles)

	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	task := NewTask(sstFiles, outputPath)
	task.MergedFiles = mergedFiles
	cm.CurrentTask = &Progress{
		InputFiles: sstFiles,
		OutputFile: outputPath,
		Status:     "completed",
		Progress:   100,
		StartedAt:  time.Now(),
	}

	if err := cm.Commit(task); err != nil {
		_ = cm.UnmarkWriting(outputPath)
		metrics.Incr(metrics.CompactionErrors, 1)
		_ = os.Remove(outputPath)
		return "", nil, fmt.Errorf("commit failed: %w", err)
	}

	// 旧文件已删除，移除 .writing 标记，此时新文件才对查询可见
	_ = cm.UnmarkWriting(outputPath)

	metrics.Incr(metrics.CompactionTotal, 1)
	metrics.Incr(metrics.CompactionInputFiles, int64(len(task.MergedFiles)))
	metrics.Incr(metrics.CompactionOutputCount, int64(task.OutputCount))
	metrics.Incr(metrics.CompactionDupCount, int64(task.DuplicateCount))

	return task.OutputPath, task.InputFiles, nil
}

// compactWithTwoPhase 执行合并，当输入文件数超过 twoPhaseThreshold 时使用两阶段合并。
func (cm *Manager) compactWithTwoPhase(ctx context.Context, inputFiles []string, outputPath string) ([]string, error) {
	if len(inputFiles) <= twoPhaseThreshold {
		task := NewTask(inputFiles, outputPath)
		if err := cm.Merge(ctx, task); err != nil {
			return nil, err
		}
		return task.MergedFiles, nil
	}

	// 分批合并为中间文件
	var intermediates []string
	var allMergedFiles []string
	for i := 0; i < len(inputFiles); i += twoPhaseThreshold {
		end := min(i+twoPhaseThreshold, len(inputFiles))
		batch := inputFiles[i:end]

		intermediateSeq := cm.ShardAccess.NextSSTSeq()
		intermediatePath := filepath.Join(cm.ShardAccess.DataDir(), fmt.Sprintf("sst_%d.bin", intermediateSeq))

		task := NewTask(batch, intermediatePath)
		if err := cm.Merge(ctx, task); err != nil {
			// 清理已创建的中间文件
			for _, p := range intermediates {
				_ = os.Remove(p)
			}
			_ = os.Remove(intermediatePath)
			return nil, fmt.Errorf("phase 1 merge batch %d: %w", i/twoPhaseThreshold, err)
		}
		intermediates = append(intermediates, intermediatePath)
		allMergedFiles = append(allMergedFiles, task.MergedFiles...)
	}

	// 合并中间文件为最终输出
	task := NewTask(intermediates, outputPath)
	if err := cm.Merge(ctx, task); err != nil {
		for _, p := range intermediates {
			_ = os.Remove(p)
		}
		return nil, fmt.Errorf("phase 2 merge: %w", err)
	}

	// 清理中间文件
	for _, p := range intermediates {
		_ = os.Remove(p)
	}

	return allMergedFiles, nil
}

// GetProgress 获取当前 compaction 进度，无活跃任务时返回 nil。
func (cm *Manager) GetProgress() *Progress {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	return cm.CurrentTask
}

// MarkWriting 开始写入标记。
func (cm *Manager) MarkWriting(sstPath string) error {
	writingFlag := sstPath + ".writing"
	f, err := storage.SafeCreate(writingFlag, 0600)
	if err != nil {
		return err
	}
	return f.Close()
}

// UnmarkWriting 结束写入标记。
func (cm *Manager) UnmarkWriting(sstPath string) error {
	writingFlag := sstPath + ".writing"
	return os.Remove(writingFlag)
}

// ResetTimer 重置定时器。
func (cm *Manager) ResetTimer() {
	cm.compactMu.Lock()
	cm.lastCompact = time.Now()
	cm.compactMu.Unlock()

	if cm.Ticker != nil {
		cm.Ticker.Reset(time.Duration(cm.Config.CheckIntervalNanos))
	}
}

// collectSSTables 收集需要 compaction 的 SSTable。
// 注意：此方法会获取所有文件的引用，防止在 CollectSSTables 和 Merge 之间被删除。
// 调用者需要在不使用后调用 ReleaseSSTRefs 释放引用。
func (cm *Manager) collectSSTablesWithRefs() ([]string, error) {
	dataDir := filepath.Join(cm.ShardAccess.Dir(), "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var sstFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}

		sstPath := filepath.Join(dataDir, entry.Name())

		// 先获取引用，防止在后续检查期间被 flush 删除
		if !cm.ShardAccess.AcquireSSTRef(sstPath) {
			slog.Warn("failed to acquire sst ref during collect, skipping", "path", sstPath)
			continue
		}

		// 再次验证 SSTable 完整性
		if cm.IsSSTableInWrite(sstPath) {
			slog.Debug("skipping sstable in write state", "path", sstPath)
			cm.ShardAccess.ReleaseSSTRef(sstPath)
			continue
		}

		if !cm.isSSTableComplete(sstPath) {
			slog.Warn("skipping incomplete sstable", "path", sstPath)
			cm.ShardAccess.ReleaseSSTRef(sstPath)
			continue
		}

		if !cm.canOpenSSTable(sstPath) {
			slog.Warn("skipping sstable: cannot open", "path", sstPath)
			cm.ShardAccess.ReleaseSSTRef(sstPath)
			continue
		}

		sstFiles = append(sstFiles, sstPath)
	}

	sort.Strings(sstFiles)
	return sstFiles, nil
}

// ReleaseSSTRefs 释放 SSTable 引用。
func (cm *Manager) ReleaseSSTRefs(paths []string) {
	for _, path := range paths {
		cm.ShardAccess.ReleaseSSTRef(path)
	}
}

// collectSSTablesWithoutRefs 收集需要 compaction 的 SSTable（不获取引用，仅用于检查数量）。
func (cm *Manager) collectSSTablesWithoutRefs() ([]string, error) {
	dataDir := filepath.Join(cm.ShardAccess.Dir(), "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var sstFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}

		sstPath := filepath.Join(dataDir, entry.Name())

		if cm.IsSSTableInWrite(sstPath) {
			continue
		}

		if !cm.isSSTableComplete(sstPath) {
			continue
		}

		sstFiles = append(sstFiles, sstPath)
	}

	sort.Strings(sstFiles)
	return sstFiles, nil
}

// isSSTableComplete 检查 SSTable 文件是否完整（不在写入中、可读、魔数正确）。
func (cm *Manager) isSSTableComplete(sstPath string) bool {
	// 如果正在写入中，不视为完整
	if cm.IsSSTableInWrite(sstPath) {
		return false
	}

	// 单文件格式：检查文件是否存在且可读
	fi, err := os.Stat(sstPath)
	if err != nil {
		return false
	}
	if !fi.Mode().IsRegular() || fi.Size() == 0 {
		return false
	}

	// 验证文件头魔数，防止正在写入的零填充文件被误认为完整
	f, err := os.Open(sstPath)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()

	var magic [8]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return false
	}
	return magic == sstable.Magic
}

// canOpenSSTable 检查 SSTable 是否可以成功打开（验证文件可访问）。
func (cm *Manager) canOpenSSTable(sstPath string) bool {
	file, err := os.Open(sstPath)
	if err != nil {
		return false
	}
	_ = file.Close()
	return true
}

// CollectSSTables 收集需要 compaction 的 SSTable（公开方法，供测试和外部调用）。
// 注意：此方法会获取引用，调用者需要在不使用后调用 ReleaseSSTRefs 释放。
func (cm *Manager) CollectSSTables() ([]string, error) {
	return cm.collectSSTablesWithRefs()
}

func (cm *Manager) IsSSTableInWrite(sstPath string) bool {
	writingFlag := sstPath + ".writing"
	_, err := os.Stat(writingFlag)
	return err == nil
}

// ShouldCompact 检查是否应该触发 compaction。
func (cm *Manager) ShouldCompact() bool {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	return cm.ShouldCompactLocked()
}

func (cm *Manager) ShouldCompactLocked() bool {
	files, err := cm.collectSSTablesWithoutRefs()
	if err != nil {
		slog.Warn("failed to collect sstables for compaction check", "error", err)
		return false
	}

	if len(files) < int(cm.Config.MaxSstableCount) {
		return false
	}

	shardSize, err := cm.CalculateShardSize()
	if err != nil {
		slog.Warn("failed to calculate shard size for compaction check", "error", err)
		return false
	}

	if shardSize >= cm.Config.ShardSizeLimit {
		slog.Info("shard size exceeds limit, skipping compaction",
			"shard", cm.ShardAccess.Dir(),
			"size", shardSize,
			"limit", cm.Config.ShardSizeLimit)
		return false
	}

	return true
}

// ShouldCompactWithLock 检查是否应该触发 compaction（调用者需持有锁）。
func (cm *Manager) ShouldCompactWithLock() bool {
	return cm.ShouldCompactLocked()
}

func (cm *Manager) CalculateShardSize() (int64, error) {
	dataDir := filepath.Join(cm.ShardAccess.Dir(), "data")
	var totalSize int64

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}
		totalSize += info.Size()
	}

	return totalSize, nil
}

// DirSize 递归计算目录下所有文件的总大小（字节），不包含子目录自身。
func DirSize(path string) (int64, error) {
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
func (cm *Manager) StartPeriodicCheck() {
	if cm.Config.CheckIntervalNanos <= 0 {
		return
	}

	cm.Ticker = time.NewTicker(time.Duration(cm.Config.CheckIntervalNanos))
	ticker := cm.Ticker
	cm.wg.Go(func() {
		for {
			select {
			case <-ticker.C:
				cm.DoPeriodicCompaction()
			case <-cm.stopCh:
				ticker.Stop()
				return
			}
		}
	})
}

// Stop 停止定期检查。
// 先 close(stopCh) + cancel context 让运行中的 compaction 感知退出，
// 再等待 goroutine 退出。
func (cm *Manager) Stop() {
	cm.stopOnce.Do(func() {
		close(cm.stopCh)
		cm.cancel()
	})
	cm.wg.Wait()
}

func (cm *Manager) DoPeriodicCompaction() {
	if !cm.TryAcquireCompactLock() {
		return
	}

	ctx, cancel := context.WithTimeout(cm.ctx, time.Duration(cm.Config.TimeoutNanos))
	defer cancel()

	if !cm.ShouldCompactLocked() {
		cm.ReleaseCompactLock()
		return
	}

	// 释放锁后调用 Compact()，避免 Compact() 内部的 TryAcquireCompactLock 自死锁
	cm.ReleaseCompactLock()

	_, _, err := cm.Compact(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Debug("periodic compaction canceled during shutdown")
		} else {
			slog.Error("periodic compaction failed", "error", err)
		}
	}

	cm.ResetTimer()
}

func (cm *Manager) TryAcquireCompactLock() bool {
	return cm.compactInProgress.CompareAndSwap(0, 1)
}

func (cm *Manager) ReleaseCompactLock() {
	cm.compactInProgress.Store(0)
}
