package measurement

import "sync"

// DatabaseMetaStore 数据库级元数据容器
type DatabaseMetaStore struct {
	mu           sync.RWMutex
	measurements map[string]*MeasurementMetaStore
}

// NewDatabaseMetaStore 创建 DatabaseMetaStore
func NewDatabaseMetaStore() *DatabaseMetaStore {
	return &DatabaseMetaStore{
		measurements: make(map[string]*MeasurementMetaStore),
	}
}

// GetOrCreate 获取或创建 MeasurementMetaStore
func (d *DatabaseMetaStore) GetOrCreate(name string) *MeasurementMetaStore {
	d.mu.RLock()
	m, ok := d.measurements[name]
	d.mu.RUnlock()
	if ok {
		return m
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	// 双重检查
	if m, ok = d.measurements[name]; ok {
		return m
	}
	m = NewMeasurementMetaStore()
	d.measurements[name] = m
	return m
}

// Close 关闭所有 MeasurementMetaStore
func (d *DatabaseMetaStore) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range d.measurements {
		_ = m.Close()
	}
	d.measurements = nil
	return nil
}
