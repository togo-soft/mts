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
	"path/filepath"
	"strconv"
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
//   - 维护 MetaStore 缓存
//
// 字段说明：
//
//   - dir:           数据存储根目录
//   - shardDuration: 分片时间窗口
//   - memTableCfg:   传递给新 Shard 的 MemTable 配置
//   - shards:        已创建的 Shard 缓存
//   - metaStores:    Measurement 级别的 MetaStore 缓存
//   - mu:            读写锁保护 shards 和 metaStores
//
// 并发安全：
//
//	所有公共方法都是线程安全的。
//	使用双检锁模式优化读性能。
type ShardManager struct {
	dir           string
	shardDuration time.Duration
	memTableCfg   MemTableConfig
	shards        map[string]*Shard
	metaStores    map[string]*measurement.MeasurementMetaStore // measurement -> metaStore
	mu            sync.RWMutex
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
func NewShardManager(dir string, shardDuration time.Duration, memTableCfg MemTableConfig) *ShardManager {
	return &ShardManager{
		dir:           dir,
		shardDuration: shardDuration,
		memTableCfg:   memTableCfg,
		shards:        make(map[string]*Shard),
		metaStores:    make(map[string]*measurement.MeasurementMetaStore),
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
		m.metaStores[metaKey] = metaStore
	}

	// 创建新 Shard
	shardDir := filepath.Join(m.dir, db, measurementName, formatTimeRange(startTime, endTime))
	s = NewShard(ShardConfig{
		DB:          db,
		Measurement: measurementName,
		StartTime:   startTime,
		EndTime:     endTime,
		Dir:         shardDir,
		MetaStore:   metaStore,
		MemTableCfg: m.memTableCfg,
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
