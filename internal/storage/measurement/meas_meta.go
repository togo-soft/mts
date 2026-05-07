// Package measurement 实现测量元数据管理。
package measurement

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// tagsHash 计算 tags 的哈希值（与顺序无关）
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

// tagsEqual 比较两个 tags 是否相等（与顺序无关）
func tagsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

// CopyTags 复制 tags map，返回副本以避免共享底层数据结构。
func CopyTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	result := make(map[string]string, len(tags))
	for k, v := range tags {
		result[k] = v
	}
	return result
}

// MeasurementMetaStore 是 Measurement 级别的元数据管理器。
type MeasurementMetaStore struct {
	mu           sync.RWMutex
	series       map[uint64]map[string]string
	tagHashIndex map[uint64]uint64
	tagIndex     map[string][]uint64
	nextSID      uint64
	dirty        bool
	persistPath  string
}

// NewMeasurementMetaStore 创建 MeasurementMetaStore。
func NewMeasurementMetaStore() *MeasurementMetaStore {
	return &MeasurementMetaStore{
		series:       make(map[uint64]map[string]string),
		tagHashIndex: make(map[uint64]uint64),
		tagIndex:     make(map[string][]uint64),
		nextSID:      0,
	}
}

// Load 从持久化文件加载元数据。
func (m *MeasurementMetaStore) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.persistPath == "" {
		return fmt.Errorf("persist path not set")
	}

	data, err := os.ReadFile(m.persistPath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	var persistData struct {
		NextSID uint64              `json:"next_sid"`
		Series  map[uint64][]string `json:"series"`
	}

	if err := json.Unmarshal(data, &persistData); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	m.series = make(map[uint64]map[string]string)
	m.tagHashIndex = make(map[uint64]uint64)
	m.tagIndex = make(map[string][]uint64)

	for sid, tagsList := range persistData.Series {
		tags := make(map[string]string)
		for i := 0; i < len(tagsList)-1; i += 2 {
			tags[tagsList[i]] = tagsList[i+1]
		}
		m.series[sid] = tags

		h := tagsHash(tags)
		m.tagHashIndex[h] = sid

		for k, v := range tags {
			indexKey := k + "\x00" + v
			m.tagIndex[indexKey] = append(m.tagIndex[indexKey], sid)
		}
	}

	m.nextSID = persistData.NextSID
	m.dirty = false
	return nil
}

// LoadFromFile 从指定路径加载元数据（不依赖 persistPath）。
func LoadFromFile(path string) (*MeasurementMetaStore, error) {
	m := NewMeasurementMetaStore()
	m.persistPath = path
	if err := m.Load(); err != nil {
		return nil, err
	}
	return m, nil
}

// SetPersistPath 设置持久化路径。
func (m *MeasurementMetaStore) SetPersistPath(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.persistPath = path
}

// AllocateSID 为指定的标签组合分配或查找 Series ID。
func (m *MeasurementMetaStore) AllocateSID(tags map[string]string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	h := tagsHash(tags)
	if sid, ok := m.tagHashIndex[h]; ok {
		if tagsEqual(m.series[sid], tags) {
			return sid, nil
		}
	}

	const maxSID = ^uint64(0)
	if m.nextSID >= maxSID {
		return 0, fmt.Errorf("series ID overflow: maximum series count reached")
	}

	sid := m.nextSID
	m.nextSID++
	m.series[sid] = CopyTags(tags)
	m.tagHashIndex[h] = sid
	m.dirty = true

	for k, v := range tags {
		indexKey := k + "\x00" + v
		m.tagIndex[indexKey] = append(m.tagIndex[indexKey], sid)
	}

	return sid, nil
}
