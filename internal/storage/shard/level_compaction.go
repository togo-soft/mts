// Package shard 实现分片存储管理。
//
// Level Compaction 机制用于合并多个 SSTable，实现层次化存储管理。
//
// 设计原则：
//
//   - 流式处理：内存高效，不一次性加载所有数据
//   - 层次结构：L0 → L1 → L2 → ... 指数增长容量
//   - 崩溃恢复：Checkpoint 机制确保安全
//   - 小文件优先：借鉴 VictoriaMetrics 策略
package shard

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

	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// LevelConfig 层次配置。
type LevelConfig struct {
	Level    int   // 层次编号 (0, 1, 2, ...)
	MaxSize  int64 // 该层最大大小（字节）
	MaxParts int   // 最大文件数（0 表示无限制）
}

// DefaultLevelConfigs 返回默认层次配置。
func DefaultLevelConfigs() []LevelConfig {
	return []LevelConfig{
		{Level: 0, MaxSize: 10 * 1024 * 1024, MaxParts: 10},        // L0: 10MB, 最多10个文件
		{Level: 1, MaxSize: 100 * 1024 * 1024, MaxParts: 0},        // L1: 100MB
		{Level: 2, MaxSize: 1024 * 1024 * 1024, MaxParts: 0},       // L2: 1GB
		{Level: 3, MaxSize: 10 * 1024 * 1024 * 1024, MaxParts: 0},  // L3: 10GB
		{Level: 4, MaxSize: 100 * 1024 * 1024 * 1024, MaxParts: 0}, // L4: 100GB
	}
}

// PartInfo 单个 Part 信息。
type PartInfo struct {
	Name      string // 文件名（不含路径）
	Size      int64  // 大小（字节）
	MinTime   int64  // 最小时间戳
	MaxTime   int64  // 最大时间戳
	DeletedAt int64  // 删除标记时间（0 表示未删除）
}

// Level 表示单个层次。
type Level struct {
	Level   int
	Parts   []PartInfo // 该层包含的 Part
	Size    int64      // 该层总大小（字节）
	MaxSize int64      // 该层大小上限
}

// LevelManifest 管理层次结构元数据。
type LevelManifest struct {
	mu     sync.RWMutex
	levels map[int]*Level // level -> Level info

	// Level 配置
	levelConfigs []LevelConfig

	// 全局序列号
	nextSeq uint64

	// Manifest 文件路径
	manifestPath string

	// 数据目录
	dataDir string
}

// NewLevelManifest 创建 LevelManifest。
func NewLevelManifest(dataDir string, configs []LevelConfig) (*LevelManifest, error) {
	if configs == nil {
		configs = DefaultLevelConfigs()
	}

	levels := make(map[int]*Level)
	for _, cfg := range configs {
		levels[cfg.Level] = &Level{
			Level:   cfg.Level,
			Parts:   make([]PartInfo, 0),
			Size:    0,
			MaxSize: cfg.MaxSize,
		}

		// 创建层次目录
		levelDir := filepath.Join(dataDir, fmt.Sprintf("L%d", cfg.Level))
		if err := storage.SafeMkdirAll(levelDir, 0700); err != nil {
			return nil, fmt.Errorf("create level dir: %w", err)
		}
	}

	manifest := &LevelManifest{
		levels:       levels,
		levelConfigs: configs,
		nextSeq:      0,
		manifestPath: filepath.Join(dataDir, "_manifest.json"),
		dataDir:      dataDir,
	}

	return manifest, nil
}

// GetLevel 获取指定层次。
func (m *LevelManifest) GetLevel(level int) *Level {
	return m.levels[level]
}

// NextSeq 返回下一个序列号并递增。
func (m *LevelManifest) NextSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	seq := m.nextSeq
	m.nextSeq++
	return seq
}

// SetNextSeq 设置下一个序列号。
func (m *LevelManifest) SetNextSeq(seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextSeq = seq
}

// AddPart 添加 Part 到指定层次。
func (m *LevelManifest) AddPart(level int, part PartInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	l, ok := m.levels[level]
	if !ok {
		return
	}

	l.Parts = append(l.Parts, part)
	l.Size += part.Size
}

// RemovePart 从指定层次删除 Part。
func (m *LevelManifest) RemovePart(level int, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	l, ok := m.levels[level]
	if !ok {
		return
	}

	for i, p := range l.Parts {
		if p.Name == name {
			l.Size -= p.Size
			l.Parts = append(l.Parts[:i], l.Parts[i+1:]...)
			return
		}
	}
}

// RemoveParts 批量删除 Parts。
func (m *LevelManifest) RemoveParts(level int, names []string) {
	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	l, ok := m.levels[level]
	if !ok {
		return
	}

	var remaining []PartInfo
	for _, p := range l.Parts {
		if nameSet[p.Name] {
			l.Size -= p.Size
		} else {
			remaining = append(remaining, p)
		}
	}
	l.Parts = remaining
}

// LevelManifestFile Manifest 文件格式。
type LevelManifestFile struct {
	Version int                   `json:"version"`
	NextSeq uint64                `json:"next_seq"`
	Levels  map[string]LevelParts `json:"levels"`
}

// LevelParts 层次零件列表（用于 JSON 序列化）。
type LevelParts struct {
	Parts []PartPartInfo `json:"parts"`
}

// PartPartInfo Part 信息（用于 JSON 序列化）。
type PartPartInfo struct {
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	MinTime   int64  `json:"min_time"`
	MaxTime   int64  `json:"max_time"`
	DeletedAt int64  `json:"deleted_at,omitempty"`
}

// Save 保存 Manifest 到文件。
func (m *LevelManifest) Save() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	levels := make(map[string]LevelParts)
	for level, l := range m.levels {
		parts := make([]PartPartInfo, len(l.Parts))
		for i, p := range l.Parts {
			parts[i] = PartPartInfo(p)
		}
		levels[fmt.Sprintf("L%d", level)] = LevelParts{Parts: parts}
	}

	file := LevelManifestFile{
		Version: 1,
		NextSeq: m.nextSeq,
		Levels:  levels,
	}

	data, err := json.Marshal(file)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	// 写入临时文件再原子性替换
	tmpPath := m.manifestPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	if err := os.Rename(tmpPath, m.manifestPath); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}

	return nil
}

// Load 从文件加载 Manifest。
func (m *LevelManifest) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 不存在则跳过
		}
		return fmt.Errorf("read manifest: %w", err)
	}

	var file LevelManifestFile
	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("unmarshal manifest: %w", err)
	}

	// 恢复 nextSeq（直接赋值，因为 Load 已持有锁）
	m.nextSeq = file.NextSeq

	// 恢复 levels
	for levelStr, lp := range file.Levels {
		var level int
		if _, err := fmt.Sscanf(levelStr, "L%d", &level); err != nil {
			continue
		}

		if l, ok := m.levels[level]; ok {
			for _, p := range lp.Parts {
				l.Parts = append(l.Parts, PartInfo(p))
				l.Size += p.Size
			}
		}
	}

	return nil
}

// GetLevelPath 获取层次目录路径。
func (m *LevelManifest) GetLevelPath(level int) string {
	return filepath.Join(m.dataDir, fmt.Sprintf("L%d", level))
}

// LevelCompactionConfig Level Compaction 配置。
type LevelCompactionConfig struct {
	// 是否启用
	Enabled bool

	// 层次配置
	LevelConfigs []LevelConfig

	// L0 到 L1 的合并大小阈值
	L0ToL1SizeThreshold int64

	// 单次最大合并文件数
	MaxCompactionParts int

	// Tombstone 保留时间
	TombstoneRetention time.Duration

	// Compaction 检查间隔
	CheckInterval time.Duration

	// Compaction 超时时间
	Timeout time.Duration

	// 是否启用 Checkpoint
	EnableCheckpoint bool
}

// DefaultLevelCompactionConfig 返回默认配置。
func DefaultLevelCompactionConfig() *LevelCompactionConfig {
	return &LevelCompactionConfig{
		Enabled:             true,
		LevelConfigs:        DefaultLevelConfigs(),
		L0ToL1SizeThreshold: 5 * 1024 * 1024, // 5MB
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
	Level      int      `json:"level"`       // 当前 compaction 层次
	InputParts []string `json:"input_parts"` // 输入文件列表
	OutputPath string   `json:"output_path"` // 输出文件路径
	OutputSeq  uint64   `json:"output_seq"`  // 输出序列号
	MergedSize int64    `json:"merged_size"` // 已合并大小
	StartedAt  int64    `json:"started_at"`  // 开始时间戳
	Timestamp  int64    `json:"timestamp"`   // 检查点更新时间
}

// checkpointPath 返回 checkpoint 文件路径。
func (cp *CompactionCheckpoint) checkpointPath(dataDir string) string {
	return filepath.Join(dataDir, "_compaction.cp")
}

// Save 保存检查点到文件。
func (cp *CompactionCheckpoint) Save(dataDir string) error {
	cp.Timestamp = time.Now().Unix()
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	path := cp.checkpointPath(dataDir)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	return nil
}

// Load 从文件加载检查点。
func (cp *CompactionCheckpoint) Load(dataDir string) error {
	path := cp.checkpointPath(dataDir)
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
	path := cp.checkpointPath(dataDir)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove checkpoint: %w", err)
	}
	return nil
}

// LevelCompactionManager 管理 Level Compaction。
type LevelCompactionManager struct {
	shard  *Shard
	config *LevelCompactionConfig

	// Manifest 管理
	manifest *LevelManifest

	// Manifest 锁（读写锁）
	manifestMu sync.RWMutex

	// Compaction 执行状态（atomic: 0=空闲, 1=执行中）
	compactInProgress int32

	// 定时触发
	ticker   *time.Ticker
	stopCh   chan struct{}
	stopOnce sync.Once

	// 序列号锁
	seqMu sync.Mutex
}

// NewLevelCompactionManager 创建 LevelCompactionManager。
func NewLevelCompactionManager(shard *Shard, config *LevelCompactionConfig) (*LevelCompactionManager, error) {
	if config == nil {
		config = DefaultLevelCompactionConfig()
	}

	dataDir := filepath.Join(shard.Dir(), "data")

	// 创建或加载 Manifest
	manifest, err := NewLevelManifest(dataDir, config.LevelConfigs)
	if err != nil {
		return nil, fmt.Errorf("create manifest: %w", err)
	}

	// 尝试加载现有 Manifest
	if err := manifest.Load(); err != nil {
		slog.Warn("failed to load manifest, starting fresh", "error", err)
	}

	lcm := &LevelCompactionManager{
		shard:    shard,
		config:   config,
		manifest: manifest,
		stopCh:   make(chan struct{}),
	}

	return lcm, nil
}

// NextSeq 返回下一个序列号。
func (lcm *LevelCompactionManager) NextSeq() uint64 {
	lcm.seqMu.Lock()
	defer lcm.seqMu.Unlock()
	return lcm.manifest.NextSeq()
}

// AddPart 添加 Part 到指定层次。
func (lcm *LevelCompactionManager) AddPart(level int, part PartInfo) {
	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()
	lcm.manifest.AddPart(level, part)
	// 保存 manifest 以持久化新增的 Part
	if err := lcm.manifest.Save(); err != nil {
		slog.Warn("failed to save manifest after AddPart", "error", err)
	}
}

// SaveManifest 保存 manifest 到磁盘。
func (lcm *LevelCompactionManager) SaveManifest() error {
	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()
	return lcm.manifest.Save()
}

// Config 返回 Level Compaction 配置。
func (lcm *LevelCompactionManager) Config() *LevelCompactionConfig {
	return lcm.config
}

// levelMaxSize 计算指定层次的容量上限。
func (lcm *LevelCompactionManager) levelMaxSize(level int) int64 {
	for _, cfg := range lcm.config.LevelConfigs {
		if cfg.Level == level {
			return cfg.MaxSize
		}
	}
	// 默认：L1=100MB，每层10倍增长
	base := int64(100 * 1024 * 1024)
	for i := 1; i < level; i++ {
		base *= 10
	}
	return base
}

// shouldCompactLevel 检查指定层次是否需要 compaction。
func (lcm *LevelCompactionManager) shouldCompactLevel(level int) bool {
	l := lcm.manifest.GetLevel(level)
	if l == nil {
		return false
	}

	if level == 0 {
		// L0: 检查文件数或大小
		if len(l.Parts) >= lcm.config.LevelConfigs[0].MaxParts {
			return true
		}
		if l.Size >= lcm.config.L0ToL1SizeThreshold {
			return true
		}
	} else {
		// Ln (n>0): 只检查大小
		if l.Size >= lcm.levelMaxSize(level) {
			return true
		}
	}

	return false
}

// ShouldCompact 检查是否应该触发 compaction。
func (lcm *LevelCompactionManager) ShouldCompact() bool {
	lcm.manifestMu.RLock()
	defer lcm.manifestMu.RUnlock()

	// 检查每一层
	for level := 0; level < len(lcm.config.LevelConfigs); level++ {
		if lcm.shouldCompactLevel(level) {
			return true
		}
	}

	return false
}

// selectPartsForMerge 选择待合并的 Parts。
// 策略：小文件优先，保持大文件顺序读取优势。
func (lcm *LevelCompactionManager) selectPartsForMerge(level int) []PartInfo {
	l := lcm.manifest.GetLevel(level)
	if l == nil || len(l.Parts) == 0 {
		return nil
	}

	// 按文件大小排序，小的优先
	parts := make([]PartInfo, len(l.Parts))
	copy(parts, l.Parts)
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].Size < parts[j].Size
	})

	// 选择直到达到下一层容量上限的一半
	var selected []PartInfo
	var totalSize int64
	targetSize := lcm.levelMaxSize(level+1) / 2

	for _, p := range parts {
		if totalSize+p.Size > targetSize && len(selected) >= 1 {
			break
		}
		selected = append(selected, p)
		totalSize += p.Size
	}

	return selected
}

// hasOverlap 检查两个 Part 时间范围是否重叠。
func hasOverlap(p1, p2 PartInfo) bool {
	// 使用严格小于，确保首尾相接的时间范围不视为重叠
	// 例如: p1=[0,200), p2=[200,300) 视为不重叠，因为没有共享的时间戳
	return p1.MinTime < p2.MaxTime && p2.MinTime < p1.MaxTime
}

// collectOverlapParts 收集与目标 Parts 时间重叠的所有 Parts。
func (lcm *LevelCompactionManager) collectOverlapParts(level int, targets []PartInfo) []PartInfo {
	var overlaps []PartInfo
	seen := make(map[string]bool)

	for _, target := range targets {
		// 检查当前层
		current := lcm.manifest.GetLevel(level)
		if current != nil {
			for _, p := range current.Parts {
				if hasOverlap(p, target) && !seen[p.Name] {
					overlaps = append(overlaps, p)
					seen[p.Name] = true
				}
			}
		}

		// 检查下一层
		next := lcm.manifest.GetLevel(level + 1)
		if next != nil {
			for _, p := range next.Parts {
				if hasOverlap(p, target) && !seen[p.Name] {
					overlaps = append(overlaps, p)
					seen[p.Name] = true
				}
			}
		}
	}

	return overlaps
}

// Compact 执行 compaction。
func (lcm *LevelCompactionManager) Compact(ctx context.Context) (string, []string, error) {
	// 尝试获取锁
	if !atomic.CompareAndSwapInt32(&lcm.compactInProgress, 0, 1) {
		return "", nil, nil // 正在执行中
	}
	defer atomic.StoreInt32(&lcm.compactInProgress, 0)

	lcm.manifestMu.Lock()

	// 选择需要 compaction 的层次
	var targetLevel int
	for level := 0; level < len(lcm.config.LevelConfigs); level++ {
		if lcm.shouldCompactLevel(level) {
			targetLevel = level
			break
		}
	}

	// 选择要合并的 parts
	selectedParts := lcm.selectPartsForMerge(targetLevel)
	if len(selectedParts) < 2 {
		lcm.manifestMu.Unlock()
		return "", nil, nil
	}

	// 收集重叠的 parts
	overlaps := lcm.collectOverlapParts(targetLevel, selectedParts)
	if len(overlaps) == 0 {
		lcm.manifestMu.Unlock()
		return "", nil, nil
	}

	// 获取输出序列号
	outputSeq := lcm.NextSeq()
	outputPath := filepath.Join(lcm.manifest.GetLevelPath(targetLevel+1), fmt.Sprintf("sst_%d", outputSeq))

	// 保存 checkpoint
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

	// 执行合并
	inputPaths := make([]string, len(overlaps))
	for i, p := range overlaps {
		inputPaths[i] = filepath.Join(lcm.manifest.GetLevelPath(targetLevel), p.Name)
	}

	if err := lcm.merge(ctx, targetLevel, inputPaths, outputPath); err != nil {
		// 清理 checkpoint
		if cp != nil {
			dataDir := filepath.Join(lcm.shard.Dir(), "data")
			_ = cp.Clear(dataDir)
		}
		return "", nil, fmt.Errorf("merge: %w", err)
	}

	// 提交结果
	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()

	// 从源层删除旧 parts
	inputNames := make([]string, len(overlaps))
	for i, p := range overlaps {
		inputNames[i] = p.Name
	}
	lcm.manifest.RemoveParts(targetLevel, inputNames)

	// 计算新 part 的大小
	var newPartSize int64
	if info, err := os.Stat(outputPath); err == nil {
		newPartSize = info.Size()
	}

	// 添加新 part 到目标层
	newPart := PartInfo{
		Name:    fmt.Sprintf("sst_%d", outputSeq),
		Size:    newPartSize,
		MinTime: overlaps[0].MinTime,
		MaxTime: overlaps[len(overlaps)-1].MaxTime,
	}
	lcm.manifest.AddPart(targetLevel+1, newPart)

	// 保存 Manifest
	if err := lcm.manifest.Save(); err != nil {
		return "", nil, fmt.Errorf("save manifest: %w", err)
	}

	// 删除旧文件
	for _, path := range inputPaths {
		_ = os.RemoveAll(path)
	}

	// 清理 checkpoint
	if cp != nil {
		dataDir := filepath.Join(lcm.shard.Dir(), "data")
		_ = cp.Clear(dataDir)
	}

	return outputPath, inputPaths, nil
}

// merge 执行流式合并。
func (lcm *LevelCompactionManager) merge(ctx context.Context, level int, inputPaths []string, outputPath string) error {
	// 打开所有输入文件的 Reader
	readers := make([]*sstable.Reader, 0, len(inputPaths))
	for _, path := range inputPaths {
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

	// 创建迭代器
	iterators := make([]*sstable.Iterator, 0, len(readers))
	for _, r := range readers {
		it, err := r.NewIterator()
		if err != nil {
			return err
		}
		iterators = append(iterators, it)
	}

	// K-way merge
	merged := newMergeIterator(iterators)

	// 解析输出序列号
	seq := uint64(0)
	if parts := strings.Split(filepath.Base(outputPath), "_"); len(parts) == 2 {
		_, _ = fmt.Sscanf(parts[1], "%d", &seq)
	}

	// 创建输出 Writer
	w, err := sstable.NewWriter(lcm.shard.Dir(), seq, 0)
	if err != nil {
		return err
	}

	// 用于去重：(timestamp, sid) -> 已见过的标记
	seen := make(map[string]bool)

	// 用于收集待写入的点
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

		// 去重
		if seen[key] {
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

	return w.Close()
}

// StartPeriodicCheck 启动定期检查。
func (lcm *LevelCompactionManager) StartPeriodicCheck() {
	if lcm.config.CheckInterval <= 0 {
		return
	}

	lcm.ticker = time.NewTicker(lcm.config.CheckInterval)
	go func() {
		for {
			select {
			case <-lcm.ticker.C:
				lcm.doPeriodicCompaction()
			case <-lcm.stopCh:
				lcm.ticker.Stop()
				return
			}
		}
	}()
}

// Stop 停止定期检查。
func (lcm *LevelCompactionManager) Stop() {
	lcm.stopOnce.Do(func() {
		close(lcm.stopCh)
	})
	time.Sleep(10 * time.Millisecond) // 给 goroutine 时间退出
}

// doPeriodicCompaction 定时执行的 compaction。
func (lcm *LevelCompactionManager) doPeriodicCompaction() {
	if !atomic.CompareAndSwapInt32(&lcm.compactInProgress, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&lcm.compactInProgress, 0)

	if !lcm.ShouldCompact() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), lcm.config.Timeout)
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
			return nil // 没有 checkpoint
		}
		return fmt.Errorf("load checkpoint: %w", err)
	}

	// 清理未完成的输出文件
	if cp.OutputPath != "" {
		_ = os.RemoveAll(cp.OutputPath)
	}

	// 删除 checkpoint
	_ = cp.Clear(dataDir)

	slog.Info("cleaned up incomplete compaction", "level", cp.Level)
	return nil
}

// IsOldFormat 检测是否为旧的扁平结构。
func (lcm *LevelCompactionManager) IsOldFormat() bool {
	dataDir := filepath.Join(lcm.shard.Dir(), "data")

	// 检查是否存在旧格式的 sst_* 目录（直接位于 data 目录下，不在 L0/L1 等层次目录中）
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return false // 无法读取目录，假设新格式
	}

	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") {
			return true // 发现旧格式目录
		}
	}

	return false
}

// MigrateFromOldFormat 从旧格式迁移。
func (lcm *LevelCompactionManager) MigrateFromOldFormat() error {
	dataDir := filepath.Join(lcm.shard.Dir(), "data")

	// 1. 创建 L0 目录
	l0Dir := filepath.Join(dataDir, "L0")
	if err := storage.SafeMkdirAll(l0Dir, 0700); err != nil {
		return fmt.Errorf("create L0 dir: %w", err)
	}

	// 2. 移动旧 SSTable 到 L0
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "sst_") {
			continue
		}

		oldPath := filepath.Join(dataDir, entry.Name())
		newPath := filepath.Join(l0Dir, entry.Name())

		if err := os.Rename(oldPath, newPath); err != nil {
			slog.Warn("failed to migrate SSTable", "from", oldPath, "to", newPath, "error", err)
		}
	}

	// 3. 初始化 Manifest
	if err := lcm.manifest.Save(); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}

	return nil
}
