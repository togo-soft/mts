package metadata

// ShardInfo 描述单个 Shard 的元数据。
type ShardInfo struct {
	ID           string `json:"id"`
	StartTime    int64  `json:"start_time"`
	EndTime      int64  `json:"end_time"`
	DataDir      string `json:"data_dir"`
	SSTableCount int    `json:"sstable_count"`
	TotalSize    int64  `json:"total_size"`
	CreatedAt    int64  `json:"created_at"`
}

// ShardIndex 管理 Shard 时间范围索引。
type ShardIndex interface {
	RegisterShard(database, measurement string, info ShardInfo) error
	UnregisterShard(database, measurement string, shardID string) error
	QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo
	ListShards(database, measurement string) []ShardInfo
	UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error
}
