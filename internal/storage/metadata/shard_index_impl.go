package metadata

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
)

// ===================================
// inMemoryShardIndex — ShardIndex 的内存实现
// ===================================

type inMemoryShardIndex struct {
	m      *Manager
	mu     sync.RWMutex
	shards map[string][]ShardInfo // "db/meas" -> []ShardInfo
}

type shardIndexData struct {
	Shards []ShardInfo `json:"shards"`
}

func newInMemoryShardIndex(m *Manager) *inMemoryShardIndex {
	return &inMemoryShardIndex{
		m:      m,
		shards: make(map[string][]ShardInfo),
	}
}

func (idx *inMemoryShardIndex) measKey(database, measurement string) string {
	return database + "/" + measurement
}

func (idx *inMemoryShardIndex) loadLocked(db, meas string) error {
	path := filepath.Join(idx.m.metaDir, db, meas, "shards.json")
	var data shardIndexData
	if err := atomicRead(path, &data); err != nil {
		return err
	}
	if len(data.Shards) > 0 {
		idx.shards[idx.measKey(db, meas)] = data.Shards
	}
	return nil
}

func (idx *inMemoryShardIndex) persistLocked(db, meas string) error {
	key := idx.measKey(db, meas)
	shards, ok := idx.shards[key]
	if !ok || len(shards) == 0 {
		return nil
	}
	path := filepath.Join(idx.m.metaDir, db, meas, "shards.json")
	return atomicWrite(path, shardIndexData{Shards: shards})
}

func (idx *inMemoryShardIndex) RegisterShard(database, measurement string, info ShardInfo) error {
	key := idx.measKey(database, measurement)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, s := range idx.shards[key] {
		if s.ID == info.ID {
			return fmt.Errorf("shard %q already registered", info.ID)
		}
	}

	idx.shards[key] = append(idx.shards[key], info)

	sort.Slice(idx.shards[key], func(i, j int) bool {
		return idx.shards[key][i].StartTime < idx.shards[key][j].StartTime
	})

	idx.m.markDirty()
	return nil
}

func (idx *inMemoryShardIndex) UnregisterShard(database, measurement string, shardID string) error {
	key := idx.measKey(database, measurement)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	shards := idx.shards[key]
	for i, s := range shards {
		if s.ID == shardID {
			idx.shards[key] = append(shards[:i], shards[i+1:]...)
			idx.m.markDirty()
			return nil
		}
	}
	return fmt.Errorf("shard %q not found", shardID)
}

func (idx *inMemoryShardIndex) QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo {
	key := idx.measKey(database, measurement)

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []ShardInfo
	for _, s := range idx.shards[key] {
		if s.StartTime < endTime && s.EndTime > startTime {
			result = append(result, s)
		}
	}
	return result
}

func (idx *inMemoryShardIndex) ListShards(database, measurement string) []ShardInfo {
	key := idx.measKey(database, measurement)

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]ShardInfo, len(idx.shards[key]))
	copy(result, idx.shards[key])
	return result
}

func (idx *inMemoryShardIndex) UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error {
	key := idx.measKey(database, measurement)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for i, s := range idx.shards[key] {
		if s.ID == shardID {
			idx.shards[key][i].SSTableCount = sstableCount
			idx.shards[key][i].TotalSize = totalSize
			idx.m.markDirty()
			return nil
		}
	}
	return fmt.Errorf("shard %q not found", shardID)
}
