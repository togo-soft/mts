// Package measurement 实现测量元数据管理。
//
// 提供 Measurement 级别的 Series 管理，包括标签分配、SID 分配和标签索引。
//
// Series 管理：
//
//	每个唯一标签组合对应一个 Series，分配唯一的 Series ID（SID）。
//	通过标签哈希实现快速查找和去重。
//
// 索引优化：
//
//	标签倒排索引支持按标签值快速定位 Series。
//	适合标签过滤查询场景。
//
// 并发安全：
//
//	所有操作都使用读写锁保护，线程安全。
package measurement

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// MeasurementMetaStore 是 Measurement 级别的元数据管理器。
//
// 管理一个 Measurement 内所有的 Series：
//
//   - 存储每个 Series 的标签（tags）
//   - 维护标签到 Series 的倒排索引
//   - 分配和管理 Series IDs（SID）
//
// Series 模型：
//
//	每个唯一的标签键值组合定义一个 Series。
//	例如 {host="A"} 和 {host="B"} 是两个不同的 Series。
//
// 索引结构：
//
//   - series:        map[SID] → tags
//   - tagHashIndex:  map[tagsHash] → SID (用于快速查找已存在的 tags)
//   - tagIndex:      map[tagKey\0tagValue] → []SID
//
// 使用场景：
//
//	写入数据时调用 AllocateSID 获取/创建 Series。
//	查询时调用 GetSidsByTag 进行标签过滤。
type MeasurementMetaStore struct {
	mu           sync.RWMutex
	series       map[uint64]map[string]string // sid → tags
	tagHashIndex map[uint64]uint64            // tagsHash → sid (快速查找)
	tagIndex     map[string][]uint64          // tagKey\0tagValue → sids
	nextSID      uint64
	dirty        bool
	persistPath  string // 持久化路径，为空时不持久化
}

// NewMeasurementMetaStore 创建 MeasurementMetaStore。
//
// 返回：
//   - *MeasurementMetaStore: 初始化的实例，初始 SID 为 0
//
// 初始化内容：
//
//   - series:        空 map，存储 sid → tags 的映射
//   - tagHashIndex:  空 map，存储 tagsHash → sid 的映射
//   - tagIndex:      空 map，存储 tagKey\x00tagValue → sids 的映射
//   - nextSID:       0，下一个待分配的 Series ID
func NewMeasurementMetaStore() *MeasurementMetaStore {
	return &MeasurementMetaStore{
		series:       make(map[uint64]map[string]string),
		tagHashIndex: make(map[uint64]uint64),
		tagIndex:     make(map[string][]uint64),
		nextSID:      0,
	}
}

// SetPersistPath 设置持久化路径。
//
// 参数：
//   - path: 持久化文件路径
//
// 说明：
//
//	设置后，当 dirty 为 true 且 Close 或 Persist 被调用时，
//	会自动将元数据持久化到指定路径。
func (m *MeasurementMetaStore) SetPersistPath(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.persistPath = path
}

// AllocateSID 为指定的标签组合分配或查找 Series ID。
//
// 参数：
//   - tags: 标签键值对，用于唯一标识一个 Series
//
// 返回：
//   - uint64: 分配的或已存在的 SID
//   - error: SID 分配失败时返回错误（如达到上限）
//
// 语义：
//
//	如果 tags 已存在（与现有 Series 完全匹配），返回现有 SID。
//	如果不存在，分配新的 SID，存储 tags，更新 tagIndex。
//	新 SID 从 nextSID 自增获取。
//
// 并发安全：
//
//	内部加锁，线程安全。
//
// 性能优化：
//
//	使用 tagsHashIndex 进行 O(1) 查找，而非 O(n) 线性遍历。
func (m *MeasurementMetaStore) AllocateSID(tags map[string]string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 使用 hash 索引快速查找已存在的 SID
	h := tagsHash(tags)
	if sid, ok := m.tagHashIndex[h]; ok {
		// 验证 tags 是否真的相等（hash 可能有碰撞）
		if tagsEqual(m.series[sid], tags) {
			return sid, nil
		}
	}

	// 检查 SID 上限（uint64 最大值）
	const maxSID = ^uint64(0)
	if m.nextSID >= maxSID {
		return 0, fmt.Errorf("series ID overflow: maximum series count reached")
	}

	// 分配新 SID
	sid := m.nextSID
	m.nextSID++
	m.series[sid] = copyTags(tags)
	m.tagHashIndex[h] = sid
	m.dirty = true

	// 更新 tagIndex
	for k, v := range tags {
		indexKey := k + "\x00" + v
		m.tagIndex[indexKey] = append(m.tagIndex[indexKey], sid)
	}

	return sid, nil
}

// GetTagsBySID 根据 Series ID 获取标签。
//
// 参数：
//   - sid: Series ID
//
// 返回：
//   - map[string]string: 标签的副本（不会受修改影响）
//   - bool:              true 表示找到，false 表示未找到
//
// 注意：
//
//	返回的标签是副本，修改它不会影响存储的数据。
func (m *MeasurementMetaStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tags, ok := m.series[sid]
	if !ok {
		return nil, false
	}
	return copyTags(tags), true
}

// GetSidsByTag 根据标签键值查找所有匹配的 Series IDs。
//
// 参数：
//   - tagKey:   标签键
//   - tagValue: 标签值
//
// 返回：
//   - []uint64: 匹配的 SID 列表（是内部的副本，可安全修改）
//
// 用途：
//
//	用于标签过滤查询，快速定位满足条件的 Series。
//	配合 GetTagsBySID 可以获取 Series 的完整信息。
func (m *MeasurementMetaStore) GetSidsByTag(tagKey, tagValue string) []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sids := m.tagIndex[tagKey+"\x00"+tagValue]
	result := make([]uint64, len(sids))
	copy(result, sids)
	return result
}

// Close 关闭 MetaStore，释放资源。
//
// 返回：
//   - error: 关闭失败时返回错误（当前总是返回 nil）
//
// 说明：
//
//	清空 series 和 tagIndex 的 map 引用，帮助垃圾回收。
//	如果设置了 persistPath 且 dirty 为 true，会先持久化。
func (m *MeasurementMetaStore) Close() error {
	// 如果有持久化路径且有脏数据，先持久化
	if m.persistPath != "" && m.dirty {
		_ = m.Persist()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.series = nil
	m.tagIndex = nil
	return nil
}

// Persist 将元数据持久化到磁盘。
//
// 持久化内容：
//   - series: sid → tags 映射
//   - nextSID: 下一个可用的 SID
//
// 返回：
//   - error: 持久化失败时返回错误
func (m *MeasurementMetaStore) Persist() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.persistPath == "" {
		return nil // 没有设置持久化路径
	}

	if !m.dirty {
		return nil // 没有脏数据，不需要持久化
	}

	// 确保目录存在
	dir := filepath.Dir(m.persistPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	// 序列化数据
	data := struct {
		NextSID uint64                  `json:"next_sid"`
		Series map[uint64][]string      `json:"series"` // 将 tags 转换为 []string 便于 JSON
	}{
		NextSID: m.nextSID,
		Series: make(map[uint64][]string),
	}

	for sid, tags := range m.series {
		tagsList := make([]string, 0, len(tags)*2)
		for k, v := range tags {
			tagsList = append(tagsList, k, v)
		}
		data.Series[sid] = tagsList
	}

	// 写入文件
	f, err := os.Create(m.persistPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() { _ = f.Close() }()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}

	m.dirty = false
	return nil
}
