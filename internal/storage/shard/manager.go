// internal/storage/shard/manager.go
package shard

import (
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"micro-ts/internal/storage/measurement"
)

// ShardManager Shard 管理器
type ShardManager struct {
	dir           string
	shardDuration time.Duration
	memTableCfg   MemTableConfig
	shards        map[string]*Shard
	metaStores    map[string]*measurement.MeasurementMetaStore // measurement -> metaStore
	mu            sync.RWMutex
}

// NewShardManager 创建 ShardManager
func NewShardManager(dir string, shardDuration time.Duration, memTableCfg MemTableConfig) *ShardManager {
	return &ShardManager{
		dir:           dir,
		shardDuration: shardDuration,
		memTableCfg:   memTableCfg,
		shards:        make(map[string]*Shard),
		metaStores:    make(map[string]*measurement.MeasurementMetaStore),
	}
}

// GetShard 获取或创建 Shard
func (m *ShardManager) GetShard(db, measurementName string, timestamp int64) (*Shard, error) {
	// 计算时间窗口
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

	// 再次检查
	if s, ok = m.shards[key]; ok {
		return s, nil
	}

	// 获取或创建 MetaStore
	metaKey := db + "/" + measurementName
	metaStore, ok := m.metaStores[metaKey]
	if !ok {
		metaStore = measurement.NewMeasurementMetaStore()
		m.metaStores[metaKey] = metaStore
	}

	// 创建新 Shard
	shardDir := filepath.Join(m.dir, db, measurementName, formatTimeRange(startTime, endTime))
	s = NewShard(db, measurementName, startTime, endTime, shardDir, metaStore, m.memTableCfg)
	m.shards[key] = s
	return s, nil
}

// GetShards 获取与时间范围相交的所有 Shard
func (m *ShardManager) GetShards(db, measurementName string, startTime, endTime int64) []*Shard {
	var result []*Shard

	// 计算时间范围内的所有 shard start 时间
	shardDuration := int64(m.shardDuration)
	shardStart := (startTime / shardDuration) * shardDuration

	m.mu.RLock()
	defer m.mu.RUnlock()

	for ts := shardStart; ts < endTime; ts += shardDuration {
		key := m.makeKey(db, measurementName, ts)
		if s, ok := m.shards[key]; ok {
			result = append(result, s)
		}
	}

	return result
}

func (m *ShardManager) calcShardStart(timestamp int64) int64 {
	return (timestamp / int64(m.shardDuration)) * int64(m.shardDuration)
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
