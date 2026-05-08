package metadata

import (
	"fmt"
	"sort"
	"sync"
)

// SimpleSeriesStore 是一个简单的内存级 Series 存储，实现 shard.SeriesStore 接口。
//
// 用于测试和轻量场景，不依赖持久化。
// 替代原先的 measurement.MeasurementMetaStore。
type SimpleSeriesStore struct {
	mu      sync.RWMutex
	series  map[uint64]map[string]string // sid -> tags
	hashIdx map[uint64]uint64            // tagsHash -> sid
	nextSID uint64
}

// NewSimpleSeriesStore 创建一个新的 SimpleSeriesStore。
func NewSimpleSeriesStore() *SimpleSeriesStore {
	return &SimpleSeriesStore{
		series:  make(map[uint64]map[string]string),
		hashIdx: make(map[uint64]uint64),
	}
}

// AllocateSID 为指定的标签组合分配或查找 Series ID。
func (s *SimpleSeriesStore) AllocateSID(tags map[string]string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h := tagsHash(tags)
	if sid, ok := s.hashIdx[h]; ok {
		if tagsEqualSimple(s.series[sid], tags) {
			return sid, nil
		}
	}

	const maxSID = ^uint64(0)
	if s.nextSID >= maxSID {
		return 0, fmt.Errorf("series ID overflow: maximum series count reached")
	}

	sid := s.nextSID
	s.nextSID++
	s.series[sid] = copyTagsMap(tags)
	s.hashIdx[h] = sid
	return sid, nil
}

// GetTagsBySID 根据 SID 查询对应的 tags。
func (s *SimpleSeriesStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tags, ok := s.series[sid]
	if !ok {
		return nil, false
	}
	return copyTagsMap(tags), true
}

// tagsHash 计算 tags 的哈希值（与顺序无关）。
func tagsHash(tags map[string]string) uint64 {
	if len(tags) == 0 {
		return 0
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var h uint64 = 14695981039346656037
	for _, k := range keys {
		v := tags[k]
		for i := 0; i < len(k); i++ {
			h ^= uint64(k[i])
			h *= 1099511628211
		}
		for i := 0; i < len(v); i++ {
			h ^= uint64(v[i])
			h *= 1099511628211
		}
	}
	return h
}

// tagsEqualSimple 比较两个 tags 是否相等。
func tagsEqualSimple(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// copyTagsMap 复制 tags map。
func copyTagsMap(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	result := make(map[string]string, len(tags))
	for k, v := range tags {
		result[k] = v
	}
	return result
}
