// Package shard 实现分片存储管理。
//
// Level Compaction 机制用于合并多个 SSTable，实现层次化存储管理。
package shard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// LevelConfig 层次配置。
type LevelConfig struct {
	Level    int
	MaxSize  int64
	MaxParts int
}

// DefaultLevelConfigs 返回默认层次配置。
func DefaultLevelConfigs() []LevelConfig {
	return []LevelConfig{
		{Level: 0, MaxSize: 10 * 1024 * 1024, MaxParts: 10},
		{Level: 1, MaxSize: 100 * 1024 * 1024, MaxParts: 0},
		{Level: 2, MaxSize: 1024 * 1024 * 1024, MaxParts: 0},
		{Level: 3, MaxSize: 10 * 1024 * 1024 * 1024, MaxParts: 0},
		{Level: 4, MaxSize: 100 * 1024 * 1024 * 1024, MaxParts: 0},
	}
}

// PartInfo 单个 Part 信息。
type PartInfo struct {
	Name      string
	Size      int64
	MinTime   int64
	MaxTime   int64
	DeletedAt int64
}

// Level 表示单个层次。
type Level struct {
	Level   int
	Parts   []PartInfo
	Size    int64
	MaxSize int64
}

// LevelManifest 管理层次结构元数据。
type LevelManifest struct {
	mu           sync.RWMutex
	levels       map[int]*Level
	levelConfigs []LevelConfig
	nextSeq      uint64
	manifestPath string
	dataDir      string
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

// getLevel 获取指定层次（调用者需持有 m.mu 的读锁或写锁）。
func (m *LevelManifest) getLevel(level int) *Level {
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
			return nil
		}
		return fmt.Errorf("read manifest: %w", err)
	}

	var file LevelManifestFile
	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("unmarshal manifest: %w", err)
	}

	m.nextSeq = file.NextSeq

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
