package shard

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
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
	shard    *Shard
	config   *LevelCompactionConfig
	manifest *LevelManifest

	manifestMu        sync.RWMutex
	compactInProgress int32

	ticker   *time.Ticker
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	seqMu sync.Mutex
}

// NewLevelCompactionManager 创建 LevelCompactionManager。
func NewLevelCompactionManager(shard *Shard, config *LevelCompactionConfig) (*LevelCompactionManager, error) {
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
	base := int64(100 * 1024 * 1024)
	for i := 1; i < level; i++ {
		base *= 10
	}
	return base
}

// shouldCompactLevel 检查指定层次是否需要 compaction。
func (lcm *LevelCompactionManager) shouldCompactLevel(level int) bool {
	l := lcm.manifest.getLevel(level)
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

	for level := 0; level < len(lcm.config.LevelConfigs); level++ {
		if lcm.shouldCompactLevel(level) {
			return true
		}
	}

	return false
}

// selectPartsForMerge 选择待合并的 Parts（小文件优先）。
func (lcm *LevelCompactionManager) selectPartsForMerge(level int) []PartInfo {
	l := lcm.manifest.getLevel(level)
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
	return p1.MinTime <= p2.MaxTime && p2.MinTime <= p1.MaxTime
}

// collectOverlapParts 收集与目标 Parts 时间重叠的所有 Parts。
func (lcm *LevelCompactionManager) collectOverlapParts(level int, targets []PartInfo) []PartInfo {
	var overlaps []PartInfo
	seen := make(map[string]bool)

	for _, target := range targets {
		current := lcm.manifest.getLevel(level)
		if current != nil {
			for _, p := range current.Parts {
				if hasOverlap(p, target) && !seen[p.Name] {
					overlaps = append(overlaps, p)
					seen[p.Name] = true
				}
			}
		}

		next := lcm.manifest.getLevel(level + 1)
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
