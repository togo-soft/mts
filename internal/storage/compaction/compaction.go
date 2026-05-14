package compaction

import (
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

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/internal/storage"
)

const (
	ShardSizeLimit    = 1 * 1024 * 1024 * 1024 // 1GB
	twoPhaseThreshold = 10                     // 输入 SSTable 超过此数时启用两阶段合并
)

// CompactionConfig Compaction 配置。
type CompactionConfig struct {
	MaxSSTableCount    int
	MaxCompactionBatch int
	ShardSizeLimit     int64
	CheckInterval      time.Duration
	Timeout            time.Duration
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
	InputFiles     []string
	MergedFiles    []string // 实际被合并的文件列表（仅这些文件可在 Commit 时安全删除）
	OutputPath     string
	Progress       int
	StartedAt      time.Time
	OutputCount    int
	DuplicateCount int
}

// NewCompactionTask 创建 compaction 任务。
func NewCompactionTask(inputFiles []string, outputPath string) *CompactionTask {
	return &CompactionTask{
		InputFiles: inputFiles,
		OutputPath: outputPath,
		Progress:   0,
		StartedAt:  time.Now(),
	}
}

// CompactionProgress 描述 compaction 进度。
type CompactionProgress struct {
	InputFiles []string
	OutputFile string
	Progress   int // 0-100
	Status     string
	StartedAt  time.Time
	Err        error
}

// CompactionManager 管理 Shard 的 Compaction。
type CompactionManager struct {
	ShardAccess ShardAccess
	Config      *CompactionConfig
	Mu          sync.Mutex

	Ticker            *time.Ticker
	stopCh            chan struct{}
	stopOnce          sync.Once
	wg                sync.WaitGroup
	lastCompact       time.Time
	compactMu         sync.Mutex
	compactInProgress int32
	CurrentTask       *CompactionProgress

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCompactionManager 创建 CompactionManager。
func NewCompactionManager(shard ShardAccess, config *CompactionConfig) *CompactionManager {
	if config == nil {
		config = DefaultCompactionConfig()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &CompactionManager{
		ShardAccess: shard,
		Config:      config,
		stopCh:      make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Timeout 返回 compaction 超时配置。
func (cm *CompactionManager) Timeout() time.Duration {
	return cm.Config.Timeout
}

// Context 返回 manager 的可取消 context，Stop() 时会取消。
// 调用方应使用此 context 创建子 context，以便 Stop() 能打断运行中的 compaction。
func (cm *CompactionManager) Context() context.Context {
	return cm.ctx
}

// SetConfig 运行时更新 Compaction 配置。
// 更新后自动重置 ticker 以使用新的 CheckInterval。
func (cm *CompactionManager) SetConfig(config *CompactionConfig) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	if config == nil {
		return
	}

	cm.Config = config

	if cm.Ticker != nil && config.CheckInterval > 0 {
		cm.Ticker.Reset(config.CheckInterval)
	}
}

// Compact 执行 compaction 合并。
func (cm *CompactionManager) Compact(ctx context.Context) (string, []string, error) {
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

	batchLimit := cm.Config.MaxCompactionBatch
	if batchLimit <= 0 {
		batchLimit = twoPhaseThreshold
	}
	if len(sstFiles) > batchLimit {
		sstFiles = sstFiles[:batchLimit]
	}

	outputSeq := cm.ShardAccess.NextSSTSeq()
	outputPath := filepath.Join(cm.ShardAccess.DataDir(), fmt.Sprintf("sst_%d.bin", outputSeq))

	cm.Mu.Unlock()

	// 两阶段合并：当文件数超过阈值时分批合并为中间文件，再合并中间文件。
	mergedFiles, mergeErr := cm.compactWithTwoPhase(ctx, sstFiles, outputPath)
	if mergeErr != nil {
		metrics.Incr(metrics.CompactionErrors, 1)
		cm.Mu.Lock()
		cm.CurrentTask = &CompactionProgress{Status: "failed", Err: mergeErr}
		cm.Mu.Unlock()
		cm.ReleaseSSTRefs(sstFiles)
		return "", nil, fmt.Errorf("merge failed: %w", mergeErr)
	}

	cm.ReleaseSSTRefs(sstFiles)

	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	task := NewCompactionTask(sstFiles, outputPath)
	task.MergedFiles = mergedFiles
	cm.CurrentTask = &CompactionProgress{
		InputFiles: sstFiles,
		OutputFile: outputPath,
		Status:     "completed",
		Progress:   100,
		StartedAt:  time.Now(),
	}

	if err := cm.Commit(task); err != nil {
		metrics.Incr(metrics.CompactionErrors, 1)
		_ = os.Remove(outputPath)
		return "", nil, fmt.Errorf("commit failed: %w", err)
	}

	metrics.Incr(metrics.CompactionTotal, 1)
	metrics.Incr(metrics.CompactionInputFiles, int64(len(task.MergedFiles)))
	metrics.Incr(metrics.CompactionOutputCount, int64(task.OutputCount))
	metrics.Incr(metrics.CompactionDupCount, int64(task.DuplicateCount))

	return task.OutputPath, task.InputFiles, nil
}

// compactWithTwoPhase 执行合并，当输入文件数超过 twoPhaseThreshold 时使用两阶段合并。
func (cm *CompactionManager) compactWithTwoPhase(ctx context.Context, inputFiles []string, outputPath string) ([]string, error) {
	if len(inputFiles) <= twoPhaseThreshold {
		task := NewCompactionTask(inputFiles, outputPath)
		if err := cm.Merge(ctx, task); err != nil {
			return nil, err
		}
		return task.MergedFiles, nil
	}

	// 分批合并为中间文件
	var intermediates []string
	for i := 0; i < len(inputFiles); i += twoPhaseThreshold {
		end := i + twoPhaseThreshold
		if end > len(inputFiles) {
			end = len(inputFiles)
		}
		batch := inputFiles[i:end]

		intermediateSeq := cm.ShardAccess.NextSSTSeq()
		intermediatePath := filepath.Join(cm.ShardAccess.DataDir(), fmt.Sprintf("sst_%d.bin", intermediateSeq))

		task := NewCompactionTask(batch, intermediatePath)
		if err := cm.Merge(ctx, task); err != nil {
			// 清理已创建的中间文件
			for _, p := range intermediates {
				_ = os.Remove(p)
			}
			_ = os.Remove(intermediatePath)
			return nil, fmt.Errorf("phase 1 merge batch %d: %w", i/twoPhaseThreshold, err)
		}
		intermediates = append(intermediates, intermediatePath)
	}

	// 合并中间文件为最终输出
	task := NewCompactionTask(intermediates, outputPath)
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

	return nil, nil
}

// GetProgress 获取当前 compaction 进度，无活跃任务时返回 nil。
func (cm *CompactionManager) GetProgress() *CompactionProgress {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	return cm.CurrentTask
}

// MarkWriting 开始写入标记。
func (cm *CompactionManager) MarkWriting(sstPath string) error {
	writingFlag := sstPath + ".writing"
	f, err := storage.SafeCreate(writingFlag, 0600)
	if err != nil {
		return err
	}
	return f.Close()
}

// UnmarkWriting 结束写入标记。
func (cm *CompactionManager) UnmarkWriting(sstPath string) error {
	writingFlag := sstPath + ".writing"
	return os.Remove(writingFlag)
}

// ResetTimer 重置定时器。
func (cm *CompactionManager) ResetTimer() {
	cm.compactMu.Lock()
	cm.lastCompact = time.Now()
	cm.compactMu.Unlock()

	if cm.Ticker != nil {
		cm.Ticker.Reset(cm.Config.CheckInterval)
	}
}

// collectSSTables 收集需要 compaction 的 SSTable。
// 注意：此方法会获取所有文件的引用，防止在 CollectSSTables 和 Merge 之间被删除。
// 调用者需要在不使用后调用 ReleaseSSTRefs 释放引用。
func (cm *CompactionManager) collectSSTablesWithRefs() ([]string, error) {
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
func (cm *CompactionManager) ReleaseSSTRefs(paths []string) {
	for _, path := range paths {
		cm.ShardAccess.ReleaseSSTRef(path)
	}
}

// collectSSTablesWithoutRefs 收集需要 compaction 的 SSTable（不获取引用，仅用于检查数量）。
func (cm *CompactionManager) collectSSTablesWithoutRefs() ([]string, error) {
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

// isSSTableComplete 检查 SSTable 文件是否完整（不在写入中且可读取）。
func (cm *CompactionManager) isSSTableComplete(sstPath string) bool {
	// 如果正在写入中，不视为完整
	if cm.IsSSTableInWrite(sstPath) {
		return false
	}

	// 单文件格式：检查文件是否存在且可读
	fi, err := os.Stat(sstPath)
	if err != nil {
		return false
	}
	return fi.Mode().IsRegular() && fi.Size() > 0
}

// canOpenSSTable 检查 SSTable 是否可以成功打开（验证文件可访问）。
func (cm *CompactionManager) canOpenSSTable(sstPath string) bool {
	file, err := os.Open(sstPath)
	if err != nil {
		return false
	}
	_ = file.Close()
	return true
}

// CollectSSTables 收集需要 compaction 的 SSTable（公开方法，供测试和外部调用）。
// 注意：此方法会获取引用，调用者需要在不使用后调用 ReleaseSSTRefs 释放。
func (cm *CompactionManager) CollectSSTables() ([]string, error) {
	return cm.collectSSTablesWithRefs()
}

func (cm *CompactionManager) IsSSTableInWrite(sstPath string) bool {
	writingFlag := sstPath + ".writing"
	_, err := os.Stat(writingFlag)
	return err == nil
}

// ShouldCompact 检查是否应该触发 compaction。
func (cm *CompactionManager) ShouldCompact() bool {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	return cm.ShouldCompactLocked()
}

func (cm *CompactionManager) ShouldCompactLocked() bool {
	files, err := cm.collectSSTablesWithoutRefs()
	if err != nil {
		slog.Warn("failed to collect sstables for compaction check", "error", err)
		return false
	}

	if len(files) < cm.Config.MaxSSTableCount {
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
func (cm *CompactionManager) ShouldCompactWithLock() bool {
	return cm.ShouldCompactLocked()
}

func (cm *CompactionManager) CalculateShardSize() (int64, error) {
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
func (cm *CompactionManager) StartPeriodicCheck() {
	if cm.Config.CheckInterval <= 0 {
		return
	}

	cm.Ticker = time.NewTicker(cm.Config.CheckInterval)
	ticker := cm.Ticker
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		for {
			select {
			case <-ticker.C:
				cm.DoPeriodicCompaction()
			case <-cm.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

// Stop 停止定期检查。
// 先 close(stopCh) + cancel context 让运行中的 compaction 感知退出，
// 再等待 goroutine 退出。
func (cm *CompactionManager) Stop() {
	cm.stopOnce.Do(func() {
		close(cm.stopCh)
		cm.cancel()
	})
	cm.wg.Wait()
}

func (cm *CompactionManager) DoPeriodicCompaction() {
	if !cm.TryAcquireCompactLock() {
		return
	}
	defer cm.ReleaseCompactLock()

	ctx, cancel := context.WithTimeout(cm.ctx, cm.Config.Timeout)
	defer cancel()

	if !cm.ShouldCompactLocked() {
		return
	}

	_, _, err := cm.Compact(ctx)
	if err != nil {
		slog.Error("periodic compaction failed", "error", err)
	}

	cm.ResetTimer()
}

func (cm *CompactionManager) TryAcquireCompactLock() bool {
	return atomic.CompareAndSwapInt32(&cm.compactInProgress, 0, 1)
}

func (cm *CompactionManager) ReleaseCompactLock() {
	atomic.StoreInt32(&cm.compactInProgress, 0)
}
