package compaction

import (
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
)

// ShardAccess 是 compaction 模块访问 Shard 的接口。
type ShardAccess interface {
	Dir() string
	DataDir() string
	NextSSTSeq() uint64
	IsSSTUnused(path string) bool
	GetSchema() (sstable.Schema, error)
	CompressionAlgorithm() sstable.CompressionAlgorithm
	// AcquireSSTRef 获取 SSTable 引用，防止在 Merge 期间被删除
	AcquireSSTRef(path string) bool
	ReleaseSSTRef(path string)
}
