package compaction

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage"
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
	Tombstones []Tombstone             `json:"tombstones"`
	index      map[uint64][]*Tombstone // SID → 匹配的 tombstones（运行时索引，不入盘）
}

// ShouldDelete 检查给定的 (sid, timestamp) 是否应被删除。
func (ts *TombstoneSet) ShouldDelete(sid uint64, timestamp int64) bool {
	if ts.index == nil {
		// 未构建索引，回退线性扫描（测试兼容）
		for i := range ts.Tombstones {
			t := &ts.Tombstones[i]
			if t.SID == sid && timestamp >= t.MinTime && timestamp <= t.MaxTime {
				return true
			}
		}
		return false
	}
	list := ts.index[sid]
	for _, t := range list {
		if timestamp >= t.MinTime && timestamp <= t.MaxTime {
			return true
		}
	}
	return false
}

// HasTombstones 是否有删除标记。
func (ts *TombstoneSet) HasTombstones() bool {
	return ts != nil && len(ts.Tombstones) > 0
}

// BuildIndex 构建 SID 索引，用于加速 ShouldDelete 查找。
// 调用方在 collectInputTombstones 之后必须调用此方法。
func (ts *TombstoneSet) BuildIndex() {
	if len(ts.Tombstones) == 0 {
		ts.index = nil
		return
	}
	ts.index = make(map[uint64][]*Tombstone)
	for i := range ts.Tombstones {
		t := &ts.Tombstones[i]
		ts.index[t.SID] = append(ts.index[t.SID], t)
	}
}

func LoadTombstones(partPath string) (*TombstoneSet, error) {
	tombstonePath := partPath + ".tombstones"
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

func SaveTombstones(partPath string, ts *TombstoneSet) error {
	if !ts.HasTombstones() {
		return nil
	}

	tombstonePath := partPath + ".tombstones"

	data, err := json.Marshal(ts)
	if err != nil {
		return fmt.Errorf("marshal tombstones: %w", err)
	}

	if err := storage.SafeWriteFile(tombstonePath, data, 0600); err != nil {
		return fmt.Errorf("write tombstones: %w", err)
	}
	return nil
}

func RemoveTombstones(partPath string) error {
	tombstonePath := partPath + ".tombstones"
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

	for _, l := range lcm.Manifest.levels {
		for _, p := range l.Parts {
			if p.DeletedAt > 0 {
				continue
			}

			partPath := filepath.Join(lcm.Manifest.GetLevelPath(l.Level), p.Name+".bin")
			ts, err := LoadTombstones(partPath)
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
				_ = RemoveTombstones(partPath)
			} else {
				_ = SaveTombstones(partPath, &TombstoneSet{Tombstones: active})
			}
		}
	}

	return nil
}
