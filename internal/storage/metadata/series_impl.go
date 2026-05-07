package metadata

import (
	"path/filepath"
	"sync"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
)

// ===================================
// shimSeriesStore — SeriesStore 的包装实现
// ===================================

type shimSeriesStore struct {
	m      *Manager
	mu     sync.RWMutex
	stores map[string]*measurement.MeasurementMetaStore // "db/meas" -> store
}

func newShimSeriesStore(m *Manager) *shimSeriesStore {
	return &shimSeriesStore{
		m:      m,
		stores: make(map[string]*measurement.MeasurementMetaStore),
	}
}

func (s *shimSeriesStore) storeKey(db, meas string) string {
	return db + "/" + meas
}

func (s *shimSeriesStore) getOrCreate(db, meas string) *measurement.MeasurementMetaStore {
	key := s.storeKey(db, meas)

	s.mu.RLock()
	store, ok := s.stores[key]
	s.mu.RUnlock()
	if ok {
		return store
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if store, ok = s.stores[key]; ok {
		return store
	}
	store = measurement.NewMeasurementMetaStore()
	// 设置持久化路径到 _metadata 目录
	store.SetPersistPath(filepath.Join(s.m.metaDir, db, meas, "series.json"))
	s.stores[key] = store
	return store
}

func (s *shimSeriesStore) loadLocked(db, meas string) error {
	key := s.storeKey(db, meas)
	store := measurement.NewMeasurementMetaStore()
	store.SetPersistPath(filepath.Join(s.m.metaDir, db, meas, "series.json"))
	if err := store.Load(); err != nil {
		// 尝试从旧路径迁移（{dataDir}/{db}/{meas}/meta.json）
		oldPath := filepath.Join(s.m.dataDir, db, meas, "meta.json")
		store.SetPersistPath(oldPath)
		if err2 := store.Load(); err2 != nil {
			return nil // 新旧都不存在，从零开始
		}
		// 迁移成功，更新路径
		store.SetPersistPath(filepath.Join(s.m.metaDir, db, meas, "series.json"))
	}
	s.stores[key] = store
	return nil
}

func (s *shimSeriesStore) persistLocked(db, meas string) error {
	key := s.storeKey(db, meas)
	store, ok := s.stores[key]
	if !ok {
		return nil
	}
	_ = store.Persist()
	return nil
}

func (s *shimSeriesStore) AllocateSID(database, measurement string, tags map[string]string) (uint64, error) {
	store := s.getOrCreate(database, measurement)
	s.m.markDirty()
	return store.AllocateSID(tags)
}

func (s *shimSeriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
	s.mu.RLock()
	store, ok := s.stores[s.storeKey(database, measurement)]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return store.GetTagsBySID(sid)
}

func (s *shimSeriesStore) GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64 {
	s.mu.RLock()
	store, ok := s.stores[s.storeKey(database, measurement)]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	return store.GetSidsByTag(tagKey, tagValue)
}

func (s *shimSeriesStore) SeriesCount(database, measurement string) int {
	s.mu.RLock()
	store, ok := s.stores[s.storeKey(database, measurement)]
	s.mu.RUnlock()
	if !ok {
		return 0
	}
	return store.SeriesCount()
}
