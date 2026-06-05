package metadata

import (
	"hash/maphash"
	"sync"
)

const (
	// defaultHashCacheMaxSize 默认最大缓存条目数（所有分片合计）。
	defaultHashCacheMaxSize = 100000
	// hashCacheShards 分片数量，必须是 2 的幂。
	hashCacheShards = 256
)

// hashSidCache 有界并发安全的 hash→SID 缓存。
// 使用分片减少锁竞争，FIFO 淘汰策略控制内存使用。
type hashSidCache struct {
	shards    []*hashSidShard
	shardMask uint64
	seed      maphash.Seed
}

// hashSidShard 单个缓存分片。
type hashSidShard struct {
	mu      sync.Mutex
	entries map[string]uint64
	order   []string // FIFO 队列
	maxSize int
}

// newHashSidCache 创建有界 hash→SID 缓存。
// maxSize <= 0 时使用默认值。
func newHashSidCache(maxSize int) *hashSidCache {
	if maxSize <= 0 {
		maxSize = defaultHashCacheMaxSize
	}
	shardSize := maxSize / hashCacheShards
	if shardSize < 1 {
		shardSize = 1
	}

	shards := make([]*hashSidShard, hashCacheShards)
	for i := range hashCacheShards {
		shards[i] = &hashSidShard{
			entries: make(map[string]uint64, shardSize),
			order:   make([]string, 0, shardSize),
			maxSize: shardSize,
		}
	}

	return &hashSidCache{
		shards:    shards,
		shardMask: hashCacheShards - 1,
		seed:      maphash.MakeSeed(),
	}
}

// shardKey 计算 key 所属的分片索引。
func (c *hashSidCache) shardKey(key string) uint64 {
	return maphash.String(c.seed, key) & c.shardMask
}

// load 从缓存中查找 SID。
func (c *hashSidCache) load(key string) (uint64, bool) {
	shard := c.shards[c.shardKey(key)]
	shard.mu.Lock()
	v, ok := shard.entries[key]
	shard.mu.Unlock()
	return v, ok
}

// trimShardOrder 在 order 使用率低于一半时收缩底层数组，释放内存。
func trimShardOrder(order []string, maxSize int) []string {
	if len(order) > maxSize/2 {
		return order
	}
	newOrder := make([]string, len(order), maxSize)
	copy(newOrder, order)
	return newOrder
}

func (c *hashSidCache) store(key string, sid uint64) {
	shard := c.shards[c.shardKey(key)]
	shard.mu.Lock()
	if _, exists := shard.entries[key]; exists {
		shard.entries[key] = sid
		shard.mu.Unlock()
		return
	}
	if len(shard.order) >= shard.maxSize {
		oldKey := shard.order[0]
		shard.order[0] = ""
		// 原地左移，cap 不变，避免 append 重新分配
		copy(shard.order, shard.order[1:])
		shard.order = shard.order[:shard.maxSize-1]
		delete(shard.entries, oldKey)
	}
	shard.order = append(shard.order, key)
	shard.order = trimShardOrder(shard.order, shard.maxSize)
	shard.entries[key] = sid
	shard.mu.Unlock()
}
