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
	"codeberg.org/micro-ts/mts/types"
)

// ShardManager 管理所有 Shard 的生命周期。
type ShardManager struct {
	dir             string
	shardDuration   time.Duration
	compactionCfg   *compaction.Config
	compressionAlgo types.CompressionAlgorithm
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
	compressionAlgo types.CompressionAlgorithm,
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
		CompressionAlgorithm: m.compressionAlgo,
	})
	m.shards[key] = s
	s.initCompaction(ShardConfig{
		CompactionCfg:        m.compactionCfg,
		CompressionAlgorithm: m.compressionAlgo,
	})

	if err := m.shardIndex.RegisterShard(db, measurementName, metadata.ShardInfo{
		ID:        key,
		StartTime: startTime,
		EndTime:   endTime,
		DataDir:   shardDir,
	}); err != nil {
		slog.Warn("failed to register shard", "key", key, "error", err)
		// 注册失败不影响 shard 返回，但调用方应关注
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
			CompressionAlgorithm: m.compressionAlgo,
		})
		m.shards[key] = shard
		shard.initCompaction(ShardConfig{
			CompactionCfg:        m.compactionCfg,
			CompressionAlgorithm: m.compressionAlgo,
		})
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

// L0Dir 返回指定 shard 的 L0 目录，若不存在则创建。
func (m *ShardManager) L0Dir(db, measurement string, shardStart int64) (string, error) {
	shard, err := m.GetShard(db, measurement, shardStart)
	if err != nil {
		return "", err
	}
	// SSTable 文件写入 shard 根目录下的 data/ 子目录，
	// 与 listSSTableFiles 扫描路径对齐。
	dir := shard.Dir()
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0700); err != nil {
		return "", err
	}
	return dir, nil
}

// ShardDurationNanos 返回 shard 时间窗口（纳秒）。
func (m *ShardManager) ShardDurationNanos() int64 {
	return int64(m.shardDuration)
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
// 先从 map 中移除 shard 并释放锁，再执行 Close 和目录删除，
// 避免长时间持锁阻塞其他 ShardManager 操作。
// os.RemoveAll 前双重检查 key 是否被重建，防止 TOCTOU 竞态。
func (m *ShardManager) DeleteShard(key string) error {
	m.mu.Lock()
	shard, ok := m.shards[key]
	if ok {
		delete(m.shards, key)
	}
	m.mu.Unlock()

	if !ok {
		return nil
	}

	if err := shard.Close(); err != nil {
		return fmt.Errorf("close shard: %w", err)
	}

	dir := shard.Dir()
	if dir == "" {
		return nil
	}

	// 双重检查：Close 期间可能有 GetShard 为相同时间窗口创建了新 Shard
	m.mu.RLock()
	_, recreated := m.shards[key]
	m.mu.RUnlock()
	if recreated {
		return nil
	}

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove shard dir: %w", err)
	}

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
