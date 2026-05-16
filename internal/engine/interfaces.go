// Package engine 定义存储引擎核心接口。
//
// 本文件定义了 Flusher、Catalog、SeriesStore、ShardIndex
// 四个核心接口，用于解耦 Engine 具体实现与外部依赖。
package engine

import (
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/types"
)

// ===================================
// Flusher — SSTable/Compaction 处理
// ===================================

// Flusher 管理 SSTable 写入和 Compaction。
type Flusher interface {
	Flush(db, measurement string, points []types.MemPoint) error
	Compact(db, measurement string, startTime int64) error
	GetShards(db, measurement string, startTime, endTime int64) []*shard.Shard
	CloseAll() error
	SetConfig(config *compaction.Config)
}

// ===================================
// Metadata 子接口
// ===================================

// Catalog 管理 database/measurement/schema。
type Catalog interface {
	CreateDatabase(name string) error
	DropDatabase(name string) error
	ListDatabases() []string
	DatabaseExists(name string) bool
	CreateMeasurement(database, name string) error
	DropMeasurement(database, name string) error
	ListMeasurements(database string) ([]string, error)
	MeasurementExists(database, name string) bool
	GetSchema(database, measurement string) (*metadata.Schema, error)
	SetSchema(database, measurement string, s *metadata.Schema) error
	GetRetention(database, measurement string) (time.Duration, error)
	SetRetention(database, measurement string, d time.Duration) error
}

// SeriesStore 管理 Series ID 分配和标签索引。
type SeriesStore interface {
	AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
	GetTags(database, measurement string, sid uint64) (map[string]string, bool)
	GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64
	SeriesCount(database, measurement string) int
}

// ShardIndex 管理 Shard 时间范围索引。
type ShardIndex interface {
	RegisterShard(database, measurement string, info ShardInfo) error
	UnregisterShard(database, measurement string, shardID string) error
	QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo
	ListShards(database, measurement string) []ShardInfo
	UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error
}

// ShardInfo 是 ShardIndex 使用的 Shard 元数据类型。
type ShardInfo = metadata.ShardInfo
