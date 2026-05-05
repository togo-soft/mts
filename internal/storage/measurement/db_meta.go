package measurement

import "sync"

// DatabaseMetaStore 是数据库级元数据容器。
//
// 管理一个数据库中的所有 Measurement，为每个 Measurement 提供独立的 MetaStore。
//
// 字段说明：
//
//   - mu:           保护 measurements map 的锁
//   - measurements: measurement 名称到 MeasurementMetaStore 的映射
//
// 使用场景：
//
//	数据库初始化时创建，关闭时统一清理所有 Measurement。
//	提供 GetOrCreate 接口，按需延迟创建 MetaStore。
type DatabaseMetaStore struct {
	mu           sync.RWMutex
	measurements map[string]*MeasurementMetaStore
}

// NewDatabaseMetaStore 创建数据库级元数据容器。
//
// 返回：
//   - *DatabaseMetaStore: 初始化的实例
//
// 说明：
//
//	初始化空的 measurements map。
//	实际 MeasurementMetaStore 在 GetOrCreate 时按需创建。
func NewDatabaseMetaStore() *DatabaseMetaStore {
	return &DatabaseMetaStore{
		measurements: make(map[string]*MeasurementMetaStore),
	}
}

// GetOrCreate 获取或创建指定名称的 MeasurementMetaStore。
//
// 参数：
//   - name: Measurement 名称
//
// 返回：
//   - *MeasurementMetaStore: 已存在或新创建的 MetaStore
//
// 并发安全：
//
//	使用双检锁（double-checked locking）模式确保并发安全。
//	读操作（RLock）检查是否存在，不存在时获取写锁（Lock）后再次检查。
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

// Close 关闭所有 MeasurementMetaStore。
//
// 返回：
//   - error: 关闭失败时返回错误（当前总是返回 nil）
//
// 说明：
//
//	遍历所有 measurements 调用 Close()。
//	清空 measurements map 帮助 GC 回收内存。
//	错误被忽略，确保尽可能多地关闭。
func (d *DatabaseMetaStore) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range d.measurements {
		_ = m.Close()
	}
	d.measurements = nil
	return nil
}
