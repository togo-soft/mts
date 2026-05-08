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
)

const ShardSizeLimit = 1 * 1024 * 1024 * 1024 // 1GB

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
}

// NewCompactionManager 创建 CompactionManager。
func NewCompactionManager(shard ShardAccess, config *CompactionConfig) *CompactionManager {
	if config == nil {
		config = DefaultCompactionConfig()
	}
	return &CompactionManager{
		ShardAccess: shard,
		Config:      config,
		stopCh:      make(chan struct{}),
	}
}

// Timeout 返回 compaction 超时配置。
func (cm *CompactionManager) Timeout() time.Duration {
	return cm.Config.Timeout
}

// Compact 执行 compaction 合并。
func (cm *CompactionManager) Compact(ctx context.Context) (string, []string, error) {
	cm.Mu.Lock()

	sstFiles, err := cm.CollectSSTables()
	if err != nil {
		cm.Mu.Unlock()
		return "", nil, fmt.Errorf("collect sstables: %w", err)
	}

	if len(sstFiles) < 2 {
		cm.Mu.Unlock()
		return "", nil, nil
	}

	if cm.Config.MaxCompactionBatch > 0 && len(sstFiles) > cm.Config.MaxCompactionBatch {
		sstFiles = sstFiles[:cm.Config.MaxCompactionBatch]
	}

	outputSeq := cm.ShardAccess.NextSSTSeq()
	outputPath := filepath.Join(cm.ShardAccess.DataDir(), fmt.Sprintf("sst_%d", outputSeq))

	task := NewCompactionTask(sstFiles, outputPath)

	cm.CurrentTask = &CompactionProgress{
		InputFiles: sstFiles,
		OutputFile: outputPath,
		Status:     "running",
		StartedAt:  time.Now(),
	}

	cm.Mu.Unlock()

	if err := cm.Merge(ctx, task); err != nil {
		cm.Mu.Lock()
		cm.CurrentTask.Status = "failed"
		cm.CurrentTask.Err = err
		cm.Mu.Unlock()
		_ = os.RemoveAll(outputPath)
		return "", nil, fmt.Errorf("merge failed: %w", err)
	}

	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	cm.CurrentTask.Status = "completed"
	cm.CurrentTask.Progress = 100

	if err := cm.Commit(task); err != nil {
		_ = os.RemoveAll(outputPath)
		return "", nil, fmt.Errorf("commit failed: %w", err)
	}

	return task.OutputPath, task.InputFiles, nil
}

// GetProgress 获取当前 compaction 进度，无活跃任务时返回 nil。
func (cm *CompactionManager) GetProgress() *CompactionProgress {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()
	return cm.CurrentTask
}

// MarkWriting 开始写入标记。
func (cm *CompactionManager) MarkWriting(sstPath string) error {
	if err := os.MkdirAll(sstPath, 0700); err != nil {
		return err
	}
	writingFlag := filepath.Join(sstPath, ".writing")
	f, err := os.Create(writingFlag)
	if err != nil {
		return err
	}
	return f.Close()
}

// UnmarkWriting 结束写入标记。
func (cm *CompactionManager) UnmarkWriting(sstPath string) error {
	writingFlag := filepath.Join(sstPath, ".writing")
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
func (cm *CompactionManager) CollectSSTables() ([]string, error) {
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
		if !entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "sst_") {
			continue
		}

		sstPath := filepath.Join(dataDir, entry.Name())

		if cm.IsSSTableInWrite(sstPath) {
			slog.Debug("skipping sstable in write state", "path", sstPath)
			continue
		}

		sstFiles = append(sstFiles, sstPath)
	}

	sort.Strings(sstFiles)
	return sstFiles, nil
}

func (cm *CompactionManager) IsSSTableInWrite(sstPath string) bool {
	writingFlag := filepath.Join(sstPath, ".writing")
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
	files, err := cm.CollectSSTables()
	if err != nil {
		return false
	}

	if len(files) < cm.Config.MaxSSTableCount {
		return false
	}

	shardSize, err := cm.CalculateShardSize()
	if err != nil {
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
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "sst_") {
			continue
		}
		sstPath := filepath.Join(dataDir, entry.Name())
		size, err := DirSize(sstPath)
		if err != nil {
			continue
		}
		totalSize += size
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
func (cm *CompactionManager) Stop() {
	cm.stopOnce.Do(func() {
		close(cm.stopCh)
	})
	cm.wg.Wait()
}

func (cm *CompactionManager) DoPeriodicCompaction() {
	if !cm.TryAcquireCompactLock() {
		return
	}
	defer cm.ReleaseCompactLock()

	ctx, cancel := context.WithTimeout(context.Background(), cm.Config.Timeout)
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
