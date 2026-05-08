// Package shard 实现分片存储管理。
//
// Compaction 机制用于合并多个 SSTable，去除重复数据，优化查询性能。
package shard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	inputFiles     []string
	outputPath     string
	progress       int
	startedAt      time.Time
	outputCount    int
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
	shard  *Shard
	config *CompactionConfig
	mu     sync.Mutex

	ticker            *time.Ticker
	stopCh            chan struct{}
	stopOnce          sync.Once
	wg                sync.WaitGroup
	lastCompact       time.Time
	compactMu         sync.Mutex
	compactInProgress int32
	currentTask       *CompactionProgress
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
func (cm *CompactionManager) Compact(ctx context.Context) (string, []string, error) {
	cm.mu.Lock()

	sstFiles, err := cm.collectSSTables()
	if err != nil {
		cm.mu.Unlock()
		return "", nil, fmt.Errorf("collect sstables: %w", err)
	}

	if len(sstFiles) < 2 {
		cm.mu.Unlock()
		return "", nil, nil
	}

	if cm.config.MaxCompactionBatch > 0 && len(sstFiles) > cm.config.MaxCompactionBatch {
		sstFiles = sstFiles[:cm.config.MaxCompactionBatch]
	}

	outputSeq := cm.shard.NextSSTSeq()
	outputPath := filepath.Join(cm.shard.DataDir(), fmt.Sprintf("sst_%d", outputSeq))

	task := NewCompactionTask(sstFiles, outputPath)

	cm.currentTask = &CompactionProgress{
		InputFiles: sstFiles,
		OutputFile: outputPath,
		Status:     "running",
		StartedAt:  time.Now(),
	}

	cm.mu.Unlock()

	if err := cm.merge(ctx, task); err != nil {
		cm.mu.Lock()
		cm.currentTask.Status = "failed"
		cm.currentTask.Err = err
		cm.mu.Unlock()
		_ = os.RemoveAll(outputPath)
		return "", nil, fmt.Errorf("merge failed: %w", err)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.currentTask.Status = "completed"
	cm.currentTask.Progress = 100

	if err := cm.commit(task); err != nil {
		_ = os.RemoveAll(outputPath)
		return "", nil, fmt.Errorf("commit failed: %w", err)
	}

	return task.outputPath, task.inputFiles, nil
}

// GetProgress 获取当前 compaction 进度，无活跃任务时返回 nil。
func (cm *CompactionManager) GetProgress() *CompactionProgress {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.currentTask
}

// collectSSTables 收集需要 compaction 的 SSTable。
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

		if cm.isSSTableInWrite(sstPath) {
			slog.Debug("skipping sstable in write state", "path", sstPath)
			continue
		}

		sstFiles = append(sstFiles, sstPath)
	}

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
