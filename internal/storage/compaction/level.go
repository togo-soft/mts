package compaction

import (
	"context"
	"encoding/json"
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
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// LevelCompactionConfig Level Compaction 配置。
type LevelCompactionConfig struct {
	Enabled             bool
	LevelConfigs        []LevelConfig
	L0ToL1SizeThreshold int64
	MaxCompactionParts  int
	TombstoneRetention  time.Duration
	CheckInterval       time.Duration
	Timeout             time.Duration
	EnableCheckpoint    bool
}

// DefaultLevelCompactionConfig 返回默认配置。
func DefaultLevelCompactionConfig() *LevelCompactionConfig {
	return &LevelCompactionConfig{
		Enabled:             true,
		LevelConfigs:        DefaultLevelConfigs(),
		L0ToL1SizeThreshold: 5 * 1024 * 1024,
		MaxCompactionParts:  10,
		TombstoneRetention:  1 * time.Hour,
		CheckInterval:       5 * time.Minute,
		Timeout:             30 * time.Minute,
		EnableCheckpoint:    true,
	}
}

// CompactionCheckpoint compaction 进度检查点。
type CompactionCheckpoint struct {
	Version    int      `json:"version"`
	Level      int      `json:"level"`
	InputParts []string `json:"input_parts"`
	OutputPath string   `json:"output_path"`
	OutputSeq  uint64   `json:"output_seq"`
	MergedSize int64    `json:"merged_size"`
	StartedAt  int64    `json:"started_at"`
	Timestamp  int64    `json:"timestamp"`
}

func (cp *CompactionCheckpoint) CheckpointPath(dataDir string) string {
	return filepath.Join(dataDir, "_compaction.cp")
}

// Save 保存检查点到文件。
func (cp *CompactionCheckpoint) Save(dataDir string) error {
	cp.Timestamp = time.Now().Unix()
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	path := cp.CheckpointPath(dataDir)
	if err := storage.SafeWriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}

	return nil
}

// Load 从文件加载检查点。
func (cp *CompactionCheckpoint) Load(dataDir string) error {
	path := cp.CheckpointPath(dataDir)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, cp); err != nil {
		return fmt.Errorf("unmarshal checkpoint: %w", err)
	}

	return nil
}

// Clear 删除检查点文件。
func (cp *CompactionCheckpoint) Clear(dataDir string) error {
	path := cp.CheckpointPath(dataDir)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove checkpoint: %w", err)
	}
	return nil
}

// LevelCompactionManager 管理 Level Compaction。
type LevelCompactionManager struct {
	shard    ShardAccess
	config   *LevelCompactionConfig
	Manifest *LevelManifest

	manifestMu        sync.RWMutex
	compactInProgress atomic.Int32

	ticker   *time.Ticker
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	seqMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

// NewLevelCompactionManager 创建 LevelCompactionManager。
func NewLevelCompactionManager(shard ShardAccess, config *LevelCompactionConfig) (*LevelCompactionManager, error) {
	if config == nil {
		config = DefaultLevelCompactionConfig()
	}

	dataDir := filepath.Join(shard.Dir(), "data")

	manifest, err := NewLevelManifest(dataDir, config.LevelConfigs)
	if err != nil {
		return nil, fmt.Errorf("create manifest: %w", err)
	}

	if err := manifest.Load(); err != nil {
		slog.Warn("failed to load manifest, starting fresh", "error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	lcm := &LevelCompactionManager{
		shard:    shard,
		config:   config,
		Manifest: manifest,
		stopCh:   make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}

	return lcm, nil
}

// Timeout 返回 level compaction 超时配置。
func (lcm *LevelCompactionManager) Timeout() time.Duration {
	return lcm.config.Timeout
}

// Context 返回 manager 的可取消 context，Stop() 时会取消。
// 调用方应使用此 context 创建子 context，以便 Stop() 能打断运行中的 compaction。
func (lcm *LevelCompactionManager) Context() context.Context {
	return lcm.ctx
}

// SetConfig 运行时更新 Level Compaction 配置。
// 更新后自动重置 ticker 以使用新的 CheckInterval。
func (lcm *LevelCompactionManager) SetConfig(config *LevelCompactionConfig) {
	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()

	if config == nil {
		return
	}

	lcm.config = config

	if lcm.ticker != nil && config.CheckInterval > 0 {
		lcm.ticker.Reset(config.CheckInterval)
	}
}

// Compact 执行 compaction。
func (lcm *LevelCompactionManager) Compact(ctx context.Context) (string, []string, error) {
	if !lcm.compactInProgress.CompareAndSwap(0, 1) {
		return "", nil, nil
	}
	defer lcm.compactInProgress.Store(0)

	lcm.manifestMu.Lock()

	var targetLevel int
	for level := 0; level < len(lcm.config.LevelConfigs); level++ {
		if lcm.ShouldCompactLevel(level) {
			targetLevel = level
			break
		}
	}

	selectedParts := lcm.SelectPartsForMerge(targetLevel)
	if len(selectedParts) < 2 {
		lcm.manifestMu.Unlock()
		return "", nil, nil
	}

	overlaps := lcm.CollectOverlapParts(targetLevel, selectedParts)
	if len(overlaps) == 0 {
		lcm.manifestMu.Unlock()
		return "", nil, nil
	}

	outputSeq := lcm.NextSeq()
	outputPath := filepath.Join(lcm.Manifest.GetLevelPath(targetLevel+1), fmt.Sprintf("sst_%d.bin", outputSeq))

	var cp *CompactionCheckpoint
	if lcm.config.EnableCheckpoint {
		cp = &CompactionCheckpoint{
			Version:    1,
			Level:      targetLevel,
			OutputSeq:  outputSeq,
			OutputPath: outputPath,
			StartedAt:  time.Now().Unix(),
		}
		inputNames := make([]string, len(overlaps))
		for i, p := range overlaps {
			inputNames[i] = p.Name
		}
		cp.InputParts = inputNames

		dataDir := filepath.Join(lcm.shard.Dir(), "data")
		if err := cp.Save(dataDir); err != nil {
			slog.Warn("failed to save checkpoint", "error", err)
		}
	}

	lcm.manifestMu.Unlock()

	inputPaths := make([]string, len(overlaps))
	for i, p := range overlaps {
		inputPaths[i] = filepath.Join(lcm.Manifest.GetLevelPath(targetLevel), p.Name+".bin")
	}

	if err := lcm.merge(ctx, targetLevel, inputPaths, outputPath); err != nil {
		if cp != nil {
			dataDir := filepath.Join(lcm.shard.Dir(), "data")
			_ = cp.Clear(dataDir)
		}
		return "", nil, fmt.Errorf("merge: %w", err)
	}

	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()

	inputNames := make([]string, len(overlaps))
	for i, p := range overlaps {
		inputNames[i] = p.Name
	}
	lcm.Manifest.RemoveParts(targetLevel, inputNames)

	var newPartSize int64
	if info, err := os.Stat(outputPath); err == nil {
		newPartSize = info.Size()
	}

	newPart := PartInfo{
		Name:    fmt.Sprintf("sst_%d", outputSeq),
		Size:    newPartSize,
		MinTime: overlaps[0].MinTime,
		MaxTime: overlaps[len(overlaps)-1].MaxTime,
	}
	lcm.Manifest.AddPart(targetLevel+1, newPart)

	if err := lcm.Manifest.Save(); err != nil {
		return "", nil, fmt.Errorf("save manifest: %w", err)
	}

	for _, path := range inputPaths {
		if !lcm.shard.IsSSTUnused(path) {
			slog.Warn("sstable still in use, deferring cleanup", "path", path)
			continue
		}
		_ = os.Remove(path)
		// 清理关联的 tombstones 文件
		tombstonePath := path + ".tombstones"
		if _, err := os.Stat(tombstonePath); err == nil {
			_ = os.Remove(tombstonePath)
		}
	}

	if cp != nil {
		dataDir := filepath.Join(lcm.shard.Dir(), "data")
		_ = cp.Clear(dataDir)
	}

	metrics.Incr(metrics.CompactionTotal, 1)

	return outputPath, inputPaths, nil
}

// merge 执行流式合并。
func (lcm *LevelCompactionManager) merge(ctx context.Context, level int, inputPaths []string, outputPath string) error {
	schema, err := lcm.shard.GetSchema()
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}

	readers := make([]*sstable.Reader, 0, len(inputPaths))
	for _, path := range inputPaths {
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

	tombstones := collectInputTombstones(inputPaths)
	tombstones.BuildIndex()

	iterators := make([]*sstable.Iterator, 0, len(readers))
	for _, r := range readers {
		it, err := r.NewIterator(nil)
		if err != nil {
			return err
		}
		iterators = append(iterators, it)
	}

	merged := NewMergeIterator(iterators)

	seq := uint64(0)
	if parts := strings.Split(filepath.Base(outputPath), "_"); len(parts) == 2 {
		_, _ = fmt.Sscanf(parts[1], "%d", &seq)
	}

	w, err := sstable.NewWriter(lcm.shard.Dir(), seq, 0, lcm.shard.CompressionAlgorithm())
	if err != nil {
		return err
	}

	seen := make(map[uint64]bool)
	var pointsToWrite []types.InternalPoint

	flushBatch := func() error {
		if len(pointsToWrite) == 0 {
			return nil
		}
		if err := w.WritePoints(pointsToWrite); err != nil {
			return err
		}
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

		if seen[key] {
			continue
		}
		if tombstones.ShouldDelete(row.Sid, row.Timestamp) {
			continue
		}
		seen[key] = true

		ip := types.InternalPoint{
			Timestamp: row.Timestamp,
			Fields:    types.MapToInternalFields(row.Fields),
			Sid:       row.Sid,
		}
		pointsToWrite = append(pointsToWrite, ip)
		if len(pointsToWrite) >= mergeBatchSize {
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

	if err := flushBatch(); err != nil {
		_ = w.Close()
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	flatPath := filepath.Join(lcm.shard.Dir(), "data", fmt.Sprintf("sst_%d.bin", seq))
	if flatPath != outputPath {
		if err := os.Rename(flatPath, outputPath); err != nil {
			return fmt.Errorf("move sstable to level path: %w", err)
		}
	}

	return SaveTombstones(outputPath, tombstones)
}

// NextSeq 返回下一个序列号。
func (lcm *LevelCompactionManager) NextSeq() uint64 {
	lcm.seqMu.Lock()
	defer lcm.seqMu.Unlock()
	return lcm.Manifest.NextSeq()
}

// AddPart 添加 Part 到指定层次。
func (lcm *LevelCompactionManager) AddPart(level int, part PartInfo) {
	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()
	lcm.Manifest.AddPart(level, part)
	if err := lcm.Manifest.Save(); err != nil {
		slog.Warn("failed to save manifest after AddPart", "error", err)
	}
}

// SaveManifest 保存 manifest 到磁盘。
func (lcm *LevelCompactionManager) SaveManifest() error {
	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()
	return lcm.Manifest.Save()
}

// Config 返回 Level Compaction 配置。
func (lcm *LevelCompactionManager) Config() *LevelCompactionConfig {
	return lcm.config
}

func (lcm *LevelCompactionManager) LevelMaxSize(level int) int64 {
	for _, cfg := range lcm.config.LevelConfigs {
		if cfg.Level == level {
			return cfg.MaxSize
		}
	}
	base := int64(100 * 1024 * 1024)
	for i := 1; i < level; i++ {
		base *= 10
	}
	return base
}

func (lcm *LevelCompactionManager) ShouldCompactLevel(level int) bool {
	l := lcm.Manifest.GetLevel(level)
	if l == nil {
		return false
	}

	if level == 0 {
		if len(l.Parts) >= lcm.config.LevelConfigs[0].MaxParts {
			return true
		}
		if l.Size >= lcm.config.L0ToL1SizeThreshold {
			return true
		}
	} else {
		if l.Size >= lcm.LevelMaxSize(level) {
			return true
		}
	}

	return false
}

// ShouldCompact 检查是否应该触发 compaction。
func (lcm *LevelCompactionManager) ShouldCompact() bool {
	lcm.manifestMu.RLock()
	defer lcm.manifestMu.RUnlock()

	for level := 0; level < len(lcm.config.LevelConfigs); level++ {
		if lcm.ShouldCompactLevel(level) {
			return true
		}
	}

	return false
}

func (lcm *LevelCompactionManager) SelectPartsForMerge(level int) []PartInfo {
	l := lcm.Manifest.GetLevel(level)
	if l == nil || len(l.Parts) == 0 {
		return nil
	}

	parts := make([]PartInfo, len(l.Parts))
	copy(parts, l.Parts)
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].Size < parts[j].Size
	})

	var selected []PartInfo
	var totalSize int64
	targetSize := lcm.LevelMaxSize(level+1) / 2

	for _, p := range parts {
		if totalSize+p.Size > targetSize && len(selected) >= 1 {
			break
		}
		selected = append(selected, p)
		totalSize += p.Size
	}

	return selected
}

func HasOverlap(p1, p2 PartInfo) bool {
	return p1.MinTime <= p2.MaxTime && p2.MinTime <= p1.MaxTime
}

func (lcm *LevelCompactionManager) CollectOverlapParts(level int, targets []PartInfo) []PartInfo {
	var overlaps []PartInfo
	seen := make(map[string]bool)

	for _, target := range targets {
		current := lcm.Manifest.GetLevel(level)
		if current != nil {
			for _, p := range current.Parts {
				if HasOverlap(p, target) && !seen[p.Name] {
					overlaps = append(overlaps, p)
					seen[p.Name] = true
				}
			}
		}

		next := lcm.Manifest.GetLevel(level + 1)
		if next != nil {
			for _, p := range next.Parts {
				if HasOverlap(p, target) && !seen[p.Name] {
					overlaps = append(overlaps, p)
					seen[p.Name] = true
				}
			}
		}
	}

	return overlaps
}

// StartPeriodicCheck 启动定期检查。
func (lcm *LevelCompactionManager) StartPeriodicCheck() {
	if lcm.config.CheckInterval <= 0 {
		return
	}

	lcm.ticker = time.NewTicker(lcm.config.CheckInterval)
	ticker := lcm.ticker
	lcm.wg.Go(func() {
		for {
			select {
			case <-ticker.C:
				lcm.doPeriodicCompaction()
			case <-lcm.stopCh:
				ticker.Stop()
				return
			}
		}
	})
}

// Stop 停止定期检查。
// 先 close(stopCh) + cancel context 让运行中的 compaction 感知退出，
// 再等待 goroutine 退出。
func (lcm *LevelCompactionManager) Stop() {
	lcm.stopOnce.Do(func() {
		close(lcm.stopCh)
		lcm.cancel()
	})
	lcm.wg.Wait()
}

func (lcm *LevelCompactionManager) doPeriodicCompaction() {
	if !lcm.compactInProgress.CompareAndSwap(0, 1) {
		return
	}
	defer lcm.compactInProgress.Store(0)

	if !lcm.ShouldCompact() {
		return
	}

	ctx, cancel := context.WithTimeout(lcm.ctx, lcm.config.Timeout)
	defer cancel()

	_, _, err := lcm.Compact(ctx)
	if err != nil {
		slog.Error("periodic compaction failed", "error", err)
	}
}

// Recover 启动时恢复检查。
func (lcm *LevelCompactionManager) Recover() error {
	dataDir := filepath.Join(lcm.shard.Dir(), "data")
	cp := &CompactionCheckpoint{}

	if err := cp.Load(dataDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("load checkpoint: %w", err)
	}

	if cp.OutputPath != "" {
		_ = os.Remove(cp.OutputPath)
	}

	_ = cp.Clear(dataDir)

	slog.Info("cleaned up incomplete compaction", "level", cp.Level)
	return nil
}
