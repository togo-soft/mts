package measurement

import (
	"sort"
	"sync"
)

// tagsHash 计算 tags 的哈希值（与顺序无关）
func tagsHash(tags map[string]string) uint64 {
	if len(tags) == 0 {
		return 0
	}

	// Get sorted keys for order-independent hashing
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var h uint64 = 14695981039346656037 // FNV offset basis
	for _, k := range keys {
		v := tags[k]
		// Use FNV-1a hash combining key and value content
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

// tagsEqual 比较两个 tags 是否相等（与顺序无关）
func tagsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	// Check a subset of b
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	// Check b subset of a (redundant but explicit)
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

// copyTags 复制 tags
func copyTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	result := make(map[string]string, len(tags))
	for k, v := range tags {
		result[k] = v
	}
	return result
}

// MeasurementMetaStore Measurement 级元数据
type MeasurementMetaStore struct {
	mu       sync.RWMutex
	series   map[uint64]map[string]string // sid → tags
	tagIndex map[string][]uint64          // tagKey\0tagValue → sids
	nextSID  uint64
	dirty    bool
}

// NewMeasurementMetaStore 创建 MeasurementMetaStore
func NewMeasurementMetaStore() *MeasurementMetaStore {
	return &MeasurementMetaStore{
		series:   make(map[uint64]map[string]string),
		tagIndex: make(map[string][]uint64),
		nextSID:  0,
	}
}

// AllocateSID 分配或查找 SID
func (m *MeasurementMetaStore) AllocateSID(tags map[string]string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 查找已存在的 SID
	for sid, t := range m.series {
		if tagsEqual(t, tags) {
			return sid
		}
	}

	// 分配新 SID
	sid := m.nextSID
	m.nextSID++
	m.series[sid] = copyTags(tags)
	m.dirty = true

	// 更新 tagIndex
	for k, v := range tags {
		indexKey := k + "\x00" + v
		m.tagIndex[indexKey] = append(m.tagIndex[indexKey], sid)
	}

	return sid
}

// GetTagsBySID 根据 SID 获取 tags
func (m *MeasurementMetaStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tags, ok := m.series[sid]
	if !ok {
		return nil, false
	}
	return copyTags(tags), true
}

// GetSidsByTag 根据 tag 条件获取 sids
func (m *MeasurementMetaStore) GetSidsByTag(tagKey, tagValue string) []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sids := m.tagIndex[tagKey+"\x00"+tagValue]
	result := make([]uint64, len(sids))
	copy(result, sids)
	return result
}

// Close 关闭
func (m *MeasurementMetaStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.series = nil
	m.tagIndex = nil
	return nil
}
