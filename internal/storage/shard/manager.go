// internal/storage/shard/manager.go
package shard

import (
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// ShardManager Shard 管理器
type ShardManager struct {
	dir           string
	shardDuration time.Duration
	shards        map[string]*Shard
	mu            sync.RWMutex
}

// NewShardManager 创建 ShardManager
func NewShardManager(dir string, shardDuration time.Duration) *ShardManager {
	return &ShardManager{
		dir:           dir,
		shardDuration: shardDuration,
		shards:        make(map[string]*Shard),
	}
}

// GetShard 获取或创建 Shard
func (m *ShardManager) GetShard(db, measurement string, timestamp int64) (*Shard, error) {
	// 计算时间窗口
	startTime := m.calcShardStart(timestamp)
	endTime := startTime + int64(m.shardDuration)

	key := m.makeKey(db, measurement, startTime)

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

	// 创建新 Shard
	shardDir := filepath.Join(m.dir, db, measurement, formatTimeRange(startTime, endTime))
	s = NewShard(db, measurement, startTime, endTime, shardDir)
	m.shards[key] = s
	return s, nil
}

// GetShards 获取与时间范围相交的所有 Shard
func (m *ShardManager) GetShards(db, measurement string, startTime, endTime int64) []*Shard {
	var result []*Shard

	// 计算时间范围内的所有 shard start 时间
	shardDuration := int64(m.shardDuration)
	shardStart := (startTime / shardDuration) * shardDuration

	m.mu.RLock()
	defer m.mu.RUnlock()

	for ts := shardStart; ts < endTime; ts += shardDuration {
		key := m.makeKey(db, measurement, ts)
		if s, ok := m.shards[key]; ok {
			result = append(result, s)
		}
	}

	return result
}

func (m *ShardManager) calcShardStart(timestamp int64) int64 {
	return (timestamp / int64(m.shardDuration)) * int64(m.shardDuration)
}

func (m *ShardManager) makeKey(db, measurement string, startTime int64) string {
	return db + "/" + measurement + "/" + formatInt64(startTime)
}

func formatTimeRange(start, end int64) string {
	return formatInt64(start) + "_" + formatInt64(end)
}

func formatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}
