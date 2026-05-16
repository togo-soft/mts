// Package shard 实现分片存储管理。
//
// ShardManager 管理所有 Shard 的生命周期，提供 Flusher 风格的方法。
package shard

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// ShardManager 管理所有 Shard 的生命周期。
type ShardManager struct {
	dir             string
	shardDuration   time.Duration
	compactionCfg   *compaction.Config
	compressionAlgo sstable.CompressionAlgorithm
	catalog         metadata.Catalog
	seriesStore     metadata.SeriesStore
	shardIndex      metadata.ShardIndex
	shards          map[string]*Shard
	mu              sync.RWMutex
}

// NewShardManager 创建新的 Shard 管理器。
func NewShardManager(
	dir string,
	shardDuration time.Duration,
	compactionCfg *compaction.Config,
	compressionAlgo sstable.CompressionAlgorithm,
	catalog metadata.Catalog,
	seriesStore metadata.SeriesStore,
	shardIndex metadata.ShardIndex,
) *ShardManager {
	return &ShardManager{
		dir:             dir,
		shardDuration:   shardDuration,
		compactionCfg:   compactionCfg,
		compressionAlgo: compressionAlgo,
		catalog:         catalog,
		seriesStore:     seriesStore,
		shardIndex:      shardIndex,
		shards:          make(map[string]*Shard),
	}
}

// GetShard 获取或创建指定时间戳对应的 Shard。
func (m *ShardManager) GetShard(db, measurementName string, timestamp int64) (*Shard, error) {
	if !isNameSafe(db) || !isNameSafe(measurementName) {
		return nil, fmt.Errorf("invalid database or measurement name")
	}

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

	shardDir := filepath.Join(m.dir, db, measurementName, formatTimeRange(startTime, endTime))
	s = NewShard(ShardConfig{
		DB:                   db,
		Measurement:          measurementName,
		StartTime:            startTime,
		EndTime:              endTime,
		Dir:                  shardDir,
		SeriesStore:          m.seriesStore,
		SchemaStore:          m.catalog,
		CompactionCfg:        m.compactionCfg,
		CompressionAlgorithm: m.compressionAlgo,
	})
	m.shards[key] = s

	if err := m.shardIndex.RegisterShard(db, measurementName, metadata.ShardInfo{
		ID:        key,
		StartTime: startTime,
		EndTime:   endTime,
		DataDir:   shardDir,
	}); err != nil {
		slog.Warn("failed to register shard", "key", key, "error", err)
	}

	return s, nil
}

// GetShards 获取与指定时间范围相交的所有 Shard。
func (m *ShardManager) GetShards(db, measurementName string, startTime, endTime int64) []*Shard {
	if !isNameSafe(db) || !isNameSafe(measurementName) {
		return nil
	}
	m.discoverShardsLocked(db, measurementName)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Shard
	prefix := db + "/" + measurementName + "/"

	for key, s := range m.shards {
		if strings.HasPrefix(key, prefix) {
			if s.startTime < endTime && s.endTime > startTime {
				result = append(result, s)
			}
		}
	}

	return result
}

func (m *ShardManager) discoverShardsLocked(db, measurementName string) {
	if !isNameSafe(db) || !isNameSafe(measurementName) {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	measurementDir := filepath.Join(m.dir, db, measurementName)
	entries, err := os.ReadDir(measurementDir)
	if err != nil {
		return
	}

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
			DB:                   db,
			Measurement:          measurementName,
			StartTime:            startTime,
			EndTime:              endTime,
			Dir:                  shardDir,
			SeriesStore:          m.seriesStore,
			SchemaStore:          m.catalog,
			CompactionCfg:        m.compactionCfg,
			CompressionAlgorithm: m.compressionAlgo,
		})
		m.shards[key] = shard
	}
}

// Flush 将 MemPoint 按时间窗口分组写入对应 Shard 的 SSTable。
func (m *ShardManager) Flush(points []types.MemPoint) error {
	if len(points) == 0 {
		return nil
	}
	// 新架构中 Flush 不再直接写入 SSTable，改为写入 unordered 目录。
	// 此方法由 FlushCoordinator 调用，实际写入逻辑在 coordinator 中处理。
	return nil
}

type flushGroup struct {
	shard  *Shard
	points []types.MemPoint
}

func (m *ShardManager) groupByShard(db, measurement string, points []types.MemPoint) []flushGroup {
	shardDur := int64(m.shardDuration)
	groupMap := make(map[int64]*flushGroup)
	var groupOrder []int64

	for _, mp := range points {
		startTime := (mp.Timestamp / shardDur) * shardDur
		g, ok := groupMap[startTime]
		if !ok {
			shard, err := m.GetShard(db, measurement, mp.Timestamp)
			if err != nil {
				continue
			}
			g = &flushGroup{shard: shard, points: make([]types.MemPoint, 0, 1024)}
			groupMap[startTime] = g
			groupOrder = append(groupOrder, startTime)
		}
		g.points = append(g.points, mp)
	}

	result := make([]flushGroup, 0, len(groupOrder))
	for _, ts := range groupOrder {
		result = append(result, *groupMap[ts])
	}
	return result
}

// Compact 触发指定 shard 的后台 compaction。
func (m *ShardManager) Compact(startTime int64) error {
	// 新架构中 compaction 由定时任务触发。
	// 此方法为接口适配，具体逻辑在 compaction 包中。
	return nil
}

// FlushAll 刷新所有 Shard。
func (m *ShardManager) FlushAll() error {
	m.mu.RLock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, s)
	}
	m.mu.RUnlock()

	for _, s := range shards {
		s.TriggerCompaction()
	}
	return nil
}

// CloseAll 关闭所有 Shard，释放资源。
func (m *ShardManager) CloseAll() error {
	m.mu.RLock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, s)
	}
	m.mu.RUnlock()

	var firstErr error
	for _, s := range shards {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PersistAll 持久化所有元数据到磁盘。
func (m *ShardManager) PersistAll() error {
	// ShardManager 不再直接持有 Manager，持久化由调用方负责
	return nil
}

// SetConfig 运行时更新所有现有 Shard 的 Compaction 配置。
func (m *ShardManager) SetConfig(config *compaction.Config) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.shards {
		if s.compaction != nil {
			s.compaction.SetConfig(config)
		}
	}
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

// WaitForDiscovery 等待初始发现完成（无操作，保留兼容）。
func (m *ShardManager) WaitForDiscovery() {
	// ShardManager 不再需要 WAL replay 发现
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

// isNameSafe 检查数据库名/measurement 名不包含路径遍历字符。
func isNameSafe(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	cleaned := filepath.Clean(name)
	if cleaned == "." || cleaned == ".." {
		return false
	}
	if strings.Contains(cleaned, string(os.PathSeparator)) {
		return false
	}
	return true
}
