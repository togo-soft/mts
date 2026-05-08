package compaction

// ShardAccess 是 compaction 模块访问 Shard 的接口。
type ShardAccess interface {
	Dir() string
	DataDir() string
	NextSSTSeq() uint64
	IsSSTUnused(path string) bool
}
