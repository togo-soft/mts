package metadata

import (
	"encoding/binary"
	"encoding/json"
	"sort"
	"sync"
)

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

// tagsEqual 比较两个 tags map 是否相等。
func tagsEqual(a, b map[string]string) bool {
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

// copyTags 复制 tags map。
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

// encodeSIDKey 将 uint64 SID 编码为 BigEndian 8 字节。
func encodeSIDKey(sid uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, sid)
	return buf
}

// decodeSIDKey 从 BigEndian 8 字节解码 uint64 SID。
func decodeSIDKey(data []byte) uint64 {
	if len(data) < 8 {
		return 0
	}
	_ = data[7]
	return uint64(data[0])<<56 | uint64(data[1])<<48 | uint64(data[2])<<40 | uint64(data[3])<<32 |
		uint64(data[4])<<24 | uint64(data[5])<<16 | uint64(data[6])<<8 | uint64(data[7])
}

// encodeUint64 将 uint64 编码为 varint 字节。
func encodeUint64(v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}

// decodeUint64 从 varint 字节解码 uint64。
func decodeUint64(data []byte) uint64 {
	v, _ := binary.Uvarint(data)
	return v
}

// marshalTags 将 tags 序列化为 JSON。
func marshalTags(tags map[string]string) ([]byte, error) {
	return json.Marshal(tags)
}

// unmarshalTags 从 JSON 反序列化 tags。
func unmarshalTags(data []byte) (map[string]string, error) {
	tags := make(map[string]string)
	if err := json.Unmarshal(data, tags); err != nil {
		return nil, err
	}
	return tags, nil
}

// SimpleSeriesStore 是纯内存的 Series 存储，供外部测试使用。
// 实现 shard.SeriesStore 接口（AllocateSID + GetTagsBySID）。
type SimpleSeriesStore struct {
	mu      sync.RWMutex
	series  map[uint64]map[string]string
	hashIdx map[uint64]uint64
	tagIdx  map[string][]uint64
	nextSID uint64
}

// NewSimpleSeriesStore 创建纯内存 SeriesStore。
func NewSimpleSeriesStore() *SimpleSeriesStore {
	return &SimpleSeriesStore{
		series:  make(map[uint64]map[string]string),
		hashIdx: make(map[uint64]uint64),
		tagIdx:  make(map[string][]uint64),
	}
}

// AllocateSID 分配或查找 SID。
func (s *SimpleSeriesStore) AllocateSID(tags map[string]string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h := tagsHash(tags)
	if sid, ok := s.hashIdx[h]; ok {
		if tagsEqual(s.series[sid], tags) {
			return sid, nil
		}
	}

	sid := s.nextSID
	s.nextSID++
	s.series[sid] = copyTags(tags)
	s.hashIdx[h] = sid

	for k, v := range tags {
		key := k + "\x00" + v
		s.tagIdx[key] = append(s.tagIdx[key], sid)
	}
	return sid, nil
}

// GetTagsBySID 根据 SID 获取 tags。
func (s *SimpleSeriesStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tags, ok := s.series[sid]
	if !ok {
		return nil, false
	}
	return copyTags(tags), true
}
