// Package shard 实现分片存储管理。
//
// ShardManager 是核心协调组件，管理所有 Shard 的生命周期。
package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/metadata"
)

// ShardManager 管理所有 Shard 的生命周期。
type ShardManager struct {
	dir                    string
	shardDuration          time.Duration
	memTableCfg            *MemTableConfig
	compactionCfg          *CompactionConfig
	manager                *metadata.Manager
	shards                 map[string]*Shard
	discoveredMeasurements map[string]bool
	mu                     sync.RWMutex
}

// NewShardManager 创建新的 Shard 管理器。
func NewShardManager(dir string, shardDuration time.Duration, memTableCfg *MemTableConfig, compactionCfg *CompactionConfig, mgr *metadata.Manager) *ShardManager {
	return &ShardManager{
		dir:                    dir,
		shardDuration:          shardDuration,
		memTableCfg:            memTableCfg,
		compactionCfg:          compactionCfg,
		manager:                mgr,
		shards:                 make(map[string]*Shard),
		discoveredMeasurements: make(map[string]bool),
	}
}

// GetShard 获取或创建指定时间戳对应的 Shard。
func (m *ShardManager) GetShard(db, measurementName string, timestamp int64) (*Shard, error) {
	startTime := m.calcShardStart(timestamp)
	endTime := startTime + int64(m.shardDuration)

	key := m.makeKey(db, measurementName, startTime)

	m.mu.RLock()
	s, ok := m.shards[key]
	m.mu.RUnlock()

	if ok {
		return s, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok = m.shards[key]; ok {
		return s, nil
	}

	// 通过 Manager 获取或创建 SeriesStore
	seriesStore := m.manager.GetOrCreateSeriesStore(db, measurementName)

	shardDir := filepath.Join(m.dir, db, measurementName, formatTimeRange(startTime, endTime))
	s = NewShard(ShardConfig{
		DB:            db,
		Measurement:   measurementName,
		StartTime:     startTime,
		EndTime:       endTime,
		Dir:           shardDir,
		SeriesStore:   seriesStore,
		MemTableCfg:   m.memTableCfg,
		CompactionCfg: m.compactionCfg,
	})
	m.shards[key] = s
	return s, nil
}

// GetShards 获取与指定时间范围相交的所有 Shard。
func (m *ShardManager) GetShards(db, measurementName string, startTime, endTime int64) []*Shard {
	m.mu.RLock()
	alreadyDiscovered := m.discoveredMeasurements[db+"/"+measurementName]
	m.mu.RUnlock()

	if !alreadyDiscovered {
		m.discoverShardsLocked(db, measurementName)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Shard

	shardDuration := int64(m.shardDuration)
	shardStart := (startTime / shardDuration) * shardDuration

	for ts := shardStart; ts < endTime; ts += shardDuration {
		key := m.makeKey(db, measurementName, ts)
		if s, ok := m.shards[key]; ok {
			result = append(result, s)
		}
	}

	return result
}

func (m *ShardManager) discoverShardsLocked(db, measurementName string) {
	metaKey := db + "/" + measurementName
	m.discoveredMeasurements[metaKey] = true

	measurementDir := filepath.Join(m.dir, db, measurementName)
	entries, err := os.ReadDir(measurementDir)
	if err != nil {
		return
	}

	// 通过 Manager 获取或创建 SeriesStore
	seriesStore := m.manager.GetOrCreateSeriesStore(db, measurementName)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		parts := strings.Split(entry.Name(), "_")
		if len(parts) != 2 {
			continue
		}

		startTime, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}
		endTime, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		key := m.makeKey(db, measurementName, startTime)

		if _, ok := m.shards[key]; ok {
			continue
		}

		shardDir := filepath.Join(measurementDir, entry.Name())
		shard := NewShard(ShardConfig{
			DB:            db,
			Measurement:   measurementName,
			StartTime:     startTime,
			EndTime:       endTime,
			Dir:           shardDir,
			SeriesStore:   seriesStore,
			MemTableCfg:   m.memTableCfg,
			CompactionCfg: m.compactionCfg,
		})
		m.shards[key] = shard
	}
}

func (m *ShardManager) calcShardStart(timestamp int64) int64 {
	shardDuration := int64(m.shardDuration)
	if shardDuration <= 0 {
		return 0
	}
	return (timestamp / shardDuration) * shardDuration
}

func (m *ShardManager) makeKey(db, measurementName string, startTime int64) string {
	return db + "/" + measurementName + "/" + formatInt64(startTime)
}

func formatTimeRange(start, end int64) string {
	return formatInt64(start) + "_" + formatInt64(end)
}

func formatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}

// FlushAll 刷新所有 Shard 的 MemTable 到 SSTable。
func (m *ShardManager) FlushAll() error {
	m.mu.RLock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, s)
	}
	m.mu.RUnlock()

	var firstErr error
	for _, s := range shards {
		if err := s.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PersistAll 持久化所有元数据到磁盘（通过 Manager）。
func (m *ShardManager) PersistAll() error {
	return m.manager.Sync()
}

// GetAllShards 返回所有 Shard 的快照。
func (m *ShardManager) GetAllShards() []*Shard {
	m.mu.RLock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, s)
	}
	m.mu.RUnlock()
	return shards
}

// DeleteShard 删除指定的 Shard。
func (m *ShardManager) DeleteShard(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	shard, ok := m.shards[key]
	if !ok {
		return nil
	}

	if err := shard.Flush(); err != nil {
		return fmt.Errorf("flush shard: %w", err)
	}

	if err := shard.Close(); err != nil {
		return fmt.Errorf("close shard: %w", err)
	}

	dir := shard.Dir()
	if dir != "" {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove shard dir: %w", err)
		}
	}

	delete(m.shards, key)
	return nil
}
