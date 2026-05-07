// Package shard 实现分片存储管理。
//
// ShardManager 是核心协调组件，管理所有 Shard 的生命周期。
//
// 分片策略：
//
//	按时间窗口分片，每个 Shard 管理固定时间段的数据。
//	分片大小由 shardDuration 控制（如 7天）。
//	分片键格式：database/measurement/startTime
package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
)

// ShardManager 管理所有 Shard 的生命周期。
//
// 负责：
//
//   - 为数据点路由到正确的 Shard
//   - 按需创建新的 Shard
//   - 查询时按需发现已存在的 Shard（DiscoverShards）
//   - 维护 MetaStore 缓存
//
// Shard 发现机制：
//
//	Shard 在写入时按需创建（GetShard）。
//	Shard 在查询时按需发现（GetShards 触发 DiscoverShards）。
//	通过 discoveredMeasurements 集合避免重复扫描同一 measurement。
//
// 字段说明：
//
//   - dir:                  数据存储根目录
//   - shardDuration:        分片时间窗口
//   - memTableCfg:          传递给新 Shard 的 MemTable 配置
//   - shards:              已创建的 Shard 缓存
//   - metaStores:          Measurement 级别的 MetaStore 缓存
//   - discoveredMeasurements: 已完成目录扫描的 db/measurement 组合
//   - mu:                  读写锁保护 shards 和 metaStores
//
// 并发安全：
//
//	所有公共方法都是线程安全的。
//	使用双检锁模式优化读性能。
type ShardManager struct {
	dir                    string
	shardDuration          time.Duration
	memTableCfg            *MemTableConfig
	compactionCfg          *CompactionConfig
	shards                 map[string]*Shard
	metaStores             map[string]*measurement.MeasurementMetaStore // measurement -> metaStore
	discoveredMeasurements map[string]bool                              // db/measurement -> 已发现
	mu                     sync.RWMutex
}

// NewShardManager 创建新的 Shard 管理器。
//
// 参数：
//   - dir:           数据存储根目录
//   - shardDuration: 每个 Shard 的时间窗口长度
//   - memTableCfg:   MemTable 配置
//
// 返回：
//   - *ShardManager: 初始化后的管理器
//
// 说明：
//
//	Shard 的数据目录为：dir/{db}/{measurement}/{startTime}_{endTime}
//	MetaStore 是惰性的，第一次写入时创建。
func NewShardManager(dir string, shardDuration time.Duration, memTableCfg *MemTableConfig, compactionCfg *CompactionConfig) *ShardManager {
	return &ShardManager{
		dir:                    dir,
		shardDuration:          shardDuration,
		memTableCfg:            memTableCfg,
		compactionCfg:          compactionCfg,
		shards:                 make(map[string]*Shard),
		metaStores:             make(map[string]*measurement.MeasurementMetaStore),
		discoveredMeasurements: make(map[string]bool),
	}
}

// GetShard 获取或创建指定时间戳对应的 Shard。
//
// 参数：
//   - db:              数据库名称
//   - measurementName: Measurement 名称
//   - timestamp:       数据点的时间戳（纳秒）
//
// 返回：
//   - *Shard: 目标 Shard（已存在或新创建）
//   - error:  创建失败时返回错误
//
// 逻辑流程：
//
//  1. 根据 timestamp 计算所在时间窗口（shard start）
//  2. 生成唯一键：db/measurementName/startTime
//  3. 检查是否已存在（使用 RLock）
//  4. 不存在则创建新 Shard（使用 Lock）
//  5. 双检锁确保并发安全
//
// MetaStore：
//
//	为每个 database/measurement 组合创建独立的 MetaStore。
//	MetaStore 被所有属于该组合的 Shard 共享。
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
		// 设置持久化路径: dir/db/measurement/meta.json
		metaPath := filepath.Join(m.dir, db, measurementName, "meta.json")
		metaStore.SetPersistPath(metaPath)
		m.metaStores[metaKey] = metaStore
	}

	// 创建新 Shard
	shardDir := filepath.Join(m.dir, db, measurementName, formatTimeRange(startTime, endTime))
	s = NewShard(ShardConfig{
		DB:            db,
		Measurement:   measurementName,
		StartTime:     startTime,
		EndTime:       endTime,
		Dir:           shardDir,
		MetaStore:     metaStore,
		MemTableCfg:   m.memTableCfg,
		CompactionCfg: m.compactionCfg,
	})
	m.shards[key] = s
	return s, nil
}

// GetShards 获取与指定时间范围相交的所有 Shard。
//
// 计算时间范围内的所有 shard 起始时间，返回已存在的 Shard。
// 不会创建新的 Shard。
//
// 参数：
//   - db:             数据库名称
//   - measurementName: Measurement 名称
//   - startTime:      查询起始时间（包含，纳秒）
//   - endTime:        查询起始时间（不包含，纳秒）
//
// 返回：
//   - []*Shard: 匹配的 Shard 列表（已排序）
//
// 使用场景：
//
//	查询时调用，获取需要扫描的 Shard 列表。
//	由于只返回已存在的 Shard，空结果表示该时间范围内没有数据。
//
// GetShards 返回指定时间范围内存在的 Shard 列表。
//
// 参数：
//   - db:              数据库名称
//   - measurementName: Measurement 名称
//   - startTime:       起始时间（包含）
//   - endTime:         结束时间（不包含）
//
// 返回：
//   - []*Shard: 在时间范围内存在的 Shard 列表（按时间排序）
//
// 说明：
//
//	此方法是查找操作，不会创建新的 Shard。
//	如果指定时间范围内没有已存在的 Shard，返回空切片。
//
// 与时间范围的关系：
//
//	返回的 Shard 满足：shard.startTime <= startTime < endTime
//	但不包括完全在范围之外的 Shard。
//
// 调用方注意：
//
//	返回空切片可能表示：
//	1. 该 db/measurement 从未创建过 Shard
//	2. 已创建的 Shard 不覆盖指定的时间范围
//
//	如果需要写入，应使用 GetOrCreateShard；
//	如果只是查询，返回空时通常返回空结果而非错误。
func (m *ShardManager) GetShards(db, measurementName string, startTime, endTime int64) []*Shard {
	m.mu.RLock()
	alreadyDiscovered := m.discoveredMeasurements[db+"/"+measurementName]
	m.mu.RUnlock()

	// 如果尚未发现过此 measurement，触发按需发现
	if !alreadyDiscovered {
		m.discoverShardsLocked(db, measurementName)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Shard

	// 计算时间范围内的所有 shard start 时间
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

// discoverShardsLocked 扫描 db/measurement 目录，发现已存在的 Shard。
//
// 调用此方法前必须持有写锁。
//
// 发现流程：
//
//  1. 标记 measurement 为已发现（避免重复扫描）
//  2. 获取或创建 MetaStore
//  3. 扫描 measurement 目录，找到所有 shard 子目录
//  4. 解析目录名获取 startTime 和 endTime
//  5. 为每个 shard 创建 Shard 实例（加载 WAL replay 数据）
//  6. 将 shard 加入缓存
func (m *ShardManager) discoverShardsLocked(db, measurementName string) {
	metaKey := db + "/" + measurementName

	// 标记为已发现（即使目录不存在，也要避免重复扫描）
	m.discoveredMeasurements[metaKey] = true

	measurementDir := filepath.Join(m.dir, db, measurementName)
	entries, err := os.ReadDir(measurementDir)
	if err != nil {
		// 目录不存在或无法读取，说明没有已存在的 shard
		return
	}

	// 获取或创建 MetaStore
	metaStore, ok := m.metaStores[metaKey]
	if !ok {
		metaStore = measurement.NewMeasurementMetaStore()
		// 设置持久化路径: dir/db/measurement/meta.json
		metaPath := filepath.Join(measurementDir, "meta.json")
		metaStore.SetPersistPath(metaPath)
		m.metaStores[metaKey] = metaStore
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// 解析目录名，格式为 startTime_endTime
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

		// 跳过已缓存的 shard
		if _, ok := m.shards[key]; ok {
			continue
		}

		// 创建 Shard 实例（会加载 WAL replay 数据）
		shardDir := filepath.Join(measurementDir, entry.Name())
		shard := NewShard(ShardConfig{
			DB:            db,
			Measurement:   measurementName,
			StartTime:     startTime,
			EndTime:       endTime,
			Dir:           shardDir,
			MetaStore:     metaStore,
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
	// 计算时间窗口起始
	// 注意：对于正常时间戳（远小于 math.MaxInt64），此计算不会溢出
	// 因为 (timestamp / shardDuration) <= timestamp，而 timestamp * shardDuration >= result
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
//
// 返回：
//   - error: 如果任一 Shard 刷盘失败则返回错误
//
// 说明：
//
//	遍历所有已创建的 Shard，调用其 Flush() 方法。
//	用于优雅关闭前确保所有内存数据持久化。
//	错误被聚合，尽可能多地刷盘。
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

// PersistAllMetaStores 持久化所有 MetaStore 的脏数据。
//
// 调用每个 MetaStore 的 Persist 方法，将元数据写入磁盘。
//
// 用于 Engine.Close() 时确保所有元数据已持久化。
//
// 返回：
//   - error: 如果任一 MetaStore 持久化失败则返回错误
func (m *ShardManager) PersistAllMetaStores() error {
	m.mu.RLock()
	metaStores := make([]*measurement.MeasurementMetaStore, 0, len(m.metaStores))
	for _, metaStore := range m.metaStores {
		metaStores = append(metaStores, metaStore)
	}
	m.mu.RUnlock()

	var errs []error
	for _, metaStore := range metaStores {
		if err := metaStore.Persist(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("persist metastores: %d errors: %v", len(errs), errs)
	}
	return nil
}

// GetAllShards 返回所有 Shard 的快照。
//
// 返回：
//   - []*Shard: 所有 Shard 的切片（按 key 排序）
//
// 注意：
//
//	返回的是 Shard 快照，不持有锁。
//	调用方不应长时间持有返回的 Shard 引用。
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
//
// 参数：
//   - key: Shard 的唯一键，格式为 "db/measurement/startTime"
//
// 返回：
//   - error: 删除失败时返回错误
//
// 删除流程：
//
//  1. 先刷盘确保数据持久化
//  2. 关闭 Shard
//  3. 删除 Shard 数据目录
//  4. 从管理器的 shards map 中移除
func (m *ShardManager) DeleteShard(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	shard, ok := m.shards[key]
	if !ok {
		return nil // Shard 不存在，视为成功
	}

	// 1. 刷盘
	if err := shard.Flush(); err != nil {
		return fmt.Errorf("flush shard: %w", err)
	}

	// 2. 关闭
	if err := shard.Close(); err != nil {
		return fmt.Errorf("close shard: %w", err)
	}

	// 3. 删除数据目录
	dir := shard.Dir()
	if dir != "" {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove shard dir: %w", err)
		}
	}

	// 4. 从 map 中移除
	delete(m.shards, key)

	return nil
}
