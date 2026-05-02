// internal/storage/measurement/meta.go
package measurement

import (
	"context"
	"sync"

	"micro-ts/internal/types"
)

// MemoryMetaStore 内存实现的 MetaStore
type MemoryMetaStore struct {
	mu       sync.RWMutex
	meta     *types.MeasurementMeta
	series   map[uint64][]byte
	tagIndex map[string][]uint64
	dirty    bool
}

// NewMemoryMetaStore 创建 MemoryMetaStore
func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{
		series:   make(map[uint64][]byte),
		tagIndex: make(map[string][]uint64),
	}
}

func (m *MemoryMetaStore) GetMeta(ctx context.Context) (*types.MeasurementMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.meta, nil
}

func (m *MemoryMetaStore) SetMeta(ctx context.Context, meta *types.MeasurementMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meta = meta
	m.dirty = true
	return nil
}

func (m *MemoryMetaStore) GetSeries(ctx context.Context, sid uint64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.series[sid], nil
}

func (m *MemoryMetaStore) SetSeries(ctx context.Context, sid uint64, tags []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.series[sid] = tags
	m.dirty = true
	return nil
}

func (m *MemoryMetaStore) GetAllSeries(ctx context.Context) (map[uint64][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[uint64][]byte, len(m.series))
	for k, v := range m.series {
		result[k] = v
	}
	return result, nil
}

func (m *MemoryMetaStore) GetSidsByTag(ctx context.Context, tagKey, tagValue string) ([]uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := tagKey + "\x00" + tagValue
	return m.tagIndex[key], nil
}

func (m *MemoryMetaStore) AddTagIndex(ctx context.Context, tagKey, tagValue string, sid uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tagKey + "\x00" + tagValue
	m.tagIndex[key] = append(m.tagIndex[key], sid)
	m.dirty = true
	return nil
}

func (m *MemoryMetaStore) Persist(ctx context.Context) error {
	// TODO: 实现持久化
	return nil
}

func (m *MemoryMetaStore) Load(ctx context.Context) error {
	// TODO: 实现加载
	return nil
}

func (m *MemoryMetaStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.series = nil
	m.tagIndex = nil
	return nil
}
