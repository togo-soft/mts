package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// Manager 是所有元数据子系统的聚合入口。
type Manager struct {
	catalog    *catalogStore
	series     *seriesStore
	shardIndex *shardIndex
	db         *bolt.DB
	closeOnce  sync.Once
}

// NewManager 创建新的 Manager 实例，打开或创建 metadata.db。
func NewManager(dataDir string) (*Manager, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbPath := filepath.Join(dataDir, "metadata.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	m := &Manager{db: db}
	m.catalog = newCatalogStore(db)
	m.series = newSeriesStore(db)
	m.shardIndex = newShardIndex(db)
	return m, nil
}

// Catalog 返回 Catalog 子系统。
func (m *Manager) Catalog() Catalog { return m.catalog }

// Series 返回 SeriesStore 子系统。
func (m *Manager) Series() SeriesStore { return m.series }

// Shards 返回 ShardIndex 子系统。
func (m *Manager) Shards() ShardIndex { return m.shardIndex }

// GetOrCreateSeriesStore 创建绑定 db/meas 的 MeasSeriesStore，供 ShardManager 使用。
func (m *Manager) GetOrCreateSeriesStore(db, meas string) *MeasSeriesStore {
	return NewMeasSeriesStore(m.series, db, meas)
}

// Load 从 bbolt 重建 series 内存缓存。
func (m *Manager) Load() error {
	return m.series.rebuildCache()
}

// Sync 强制 fsync bbolt 数据库。
func (m *Manager) Sync() error {
	return m.db.Sync()
}

// Close 关闭 bbolt 数据库（仅一次）。
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		err = m.db.Close()
	})
	return err
}
