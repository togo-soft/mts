package shard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Tombstone 表示数据删除标记。
type Tombstone struct {
	SID       uint64 `json:"sid"`
	MinTime   int64  `json:"mint"`
	MaxTime   int64  `json:"maxt"`
	DeletedAt int64  `json:"deleted"`
}

// TombstoneSet 表示一组删除标记。
type TombstoneSet struct {
	Tombstones []Tombstone `json:"tombstones"`
}

// ShouldDelete 检查给定的 (sid, timestamp) 是否应被删除。
func (ts *TombstoneSet) ShouldDelete(sid uint64, timestamp int64) bool {
	for i := range ts.Tombstones {
		t := &ts.Tombstones[i]
		if t.SID == sid && timestamp >= t.MinTime && timestamp <= t.MaxTime {
			return true
		}
	}
	return false
}

// HasTombstones 是否有删除标记。
func (ts *TombstoneSet) HasTombstones() bool {
	return ts != nil && len(ts.Tombstones) > 0
}

// loadTombstones 从 Part 目录加载删除标记。
func loadTombstones(partPath string) (*TombstoneSet, error) {
	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	data, err := os.ReadFile(tombstonePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read tombstones: %w", err)
	}

	var ts TombstoneSet
	if err := json.Unmarshal(data, &ts); err != nil {
		return nil, fmt.Errorf("unmarshal tombstones: %w", err)
	}
	return &ts, nil
}

// saveTombstones 保存删除标记到 Part 目录。
func saveTombstones(partPath string, ts *TombstoneSet) error {
	if !ts.HasTombstones() {
		return nil
	}

	if err := os.MkdirAll(partPath, 0700); err != nil {
		return fmt.Errorf("create part dir: %w", err)
	}

	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	tmpPath := tombstonePath + ".tmp"

	data, err := json.Marshal(ts)
	if err != nil {
		return fmt.Errorf("marshal tombstones: %w", err)
	}

	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("write tombstones: %w", err)
	}

	if err := os.Rename(tmpPath, tombstonePath); err != nil {
		return fmt.Errorf("rename tombstones: %w", err)
	}
	return nil
}

// removeTombstones 删除 Part 目录下的删除标记文件。
func removeTombstones(partPath string) error {
	tombstonePath := filepath.Join(partPath, "_tombstones.json")
	err := os.Remove(tombstonePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove tombstones: %w", err)
	}
	return nil
}

// CompactTombstones 清理已过期的 tombstone。
func (lcm *LevelCompactionManager) CompactTombstones() error {
	lcm.manifestMu.RLock()
	defer lcm.manifestMu.RUnlock()

	retentionPeriod := lcm.config.TombstoneRetention
	now := time.Now().Unix()

	for _, l := range lcm.manifest.levels {
		for _, p := range l.Parts {
			if p.DeletedAt > 0 {
				continue
			}

			partPath := filepath.Join(lcm.manifest.GetLevelPath(l.Level), p.Name)
			ts, err := loadTombstones(partPath)
			if err != nil {
				return fmt.Errorf("load tombstones for %s: %w", p.Name, err)
			}
			if !ts.HasTombstones() {
				continue
			}

			var active []Tombstone
			for _, t := range ts.Tombstones {
				if now-t.DeletedAt < int64(retentionPeriod.Seconds()) {
					active = append(active, t)
				}
			}

			if len(active) == len(ts.Tombstones) {
				continue
			}
			if len(active) == 0 {
				_ = removeTombstones(partPath)
			} else {
				_ = saveTombstones(partPath, &TombstoneSet{Tombstones: active})
			}
		}
	}

	return nil
}
