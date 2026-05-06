package types

import (
	"time"
)

// CompactionConfig 配置 Compaction 行为。
//
// 字段说明：
//
//   - MaxSSTableCount: 最大 SSTable 数量，超过此值触发 compaction
//   - MaxCompactionBatch: 单次 compaction 最大文件数（0 表示不限制）
//   - ShardSizeLimit: 单个 Shard 数据大小上限（字节），超过后不参与 compaction
//   - CheckInterval: 定时检查间隔（0 表示禁用定时触发）
//   - Timeout: Compaction 超时时间
type CompactionConfig struct {
	// 最大 SSTable 数量，超过此值触发 compaction
	MaxSSTableCount int

	// 单次 compaction 最大文件数（0 表示不限制）
	MaxCompactionBatch int

	// 单个 Shard 数据大小上限（字节），超过后不参与 compaction
	ShardSizeLimit int64

	// 定时触发间隔（0 表示禁用定时触发）
	CheckInterval time.Duration

	// Compaction 超时时间
	Timeout time.Duration
}

// DefaultCompactionConfig 返回默认的 Compaction 配置。
func DefaultCompactionConfig() *CompactionConfig {
	return &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 0,
		ShardSizeLimit:     1 * 1024 * 1024 * 1024, // 1GB
		CheckInterval:      1 * time.Hour,
		Timeout:            30 * time.Minute,
	}
}
