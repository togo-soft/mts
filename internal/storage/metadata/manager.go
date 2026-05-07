package metadata

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
)

// manifestData 是 manifest.json 的存储结构。
type manifestData struct {
	Version   int                   `json:"version"`
	Databases map[string]dbManifest `json:"databases"`
}

type dbManifest struct {
	Measurements map[string]measManifest `json:"measurements"`
	CreatedAt    int64                   `json:"created_at"`
}

type measManifest struct {
	SchemaVersion int64 `json:"schema_version"`
	RetentionNs   int64 `json:"retention_ns,omitempty"`
	CreatedAt     int64 `json:"created_at"`
}

// Manager 是所有元数据子系统的聚合入口。
type Manager struct {
	catalog    *shimCatalog
	series     *shimSeriesStore
	shardIndex *inMemoryShardIndex
	dataDir    string
	metaDir    string
	mu         sync.RWMutex
	dbMeta     map[string]*measurement.DatabaseMetaStore
	schema     map[string]*Schema // "db/meas" -> Schema
	dirty      atomic.Bool
}

// NewManager 创建新的 Manager 实例。
func NewManager(dataDir string) (*Manager, error) {
	m := &Manager{
		dataDir: dataDir,
		metaDir: filepath.Join(dataDir, "_metadata"),
		dbMeta:  make(map[string]*measurement.DatabaseMetaStore),
		schema:  make(map[string]*Schema),
	}
	m.catalog = newShimCatalog(m)
	m.series = newShimSeriesStore(m)
	m.shardIndex = newInMemoryShardIndex(m)
	return m, nil
}

// Catalog 返回 Catalog 子系统。
func (m *Manager) Catalog() Catalog { return m.catalog }

// Series 返回 SeriesStore 子系统。
func (m *Manager) Series() SeriesStore { return m.series }

// Shards 返回 ShardIndex 子系统。
func (m *Manager) Shards() ShardIndex { return m.shardIndex }

// DataDir 返回数据目录路径。
func (m *Manager) DataDir() string { return m.dataDir }

// MetaDir 返回元数据目录路径。
func (m *Manager) MetaDir() string { return m.metaDir }

// GetOrCreateSeriesStore 获取或创建底层 MeasurementMetaStore，供 ShardManager 使用。
func (m *Manager) GetOrCreateSeriesStore(db, meas string) *measurement.MeasurementMetaStore {
	return m.series.getOrCreate(db, meas)
}

// markDirty 标记 Manager 有脏数据需要持久化（无锁，通过 atomic 操作）。
func (m *Manager) markDirty() {
	m.dirty.Store(true)
}

// Load 从磁盘加载所有元数据。
//
// 加载顺序：
//  1. manifest.json → Catalog
//  2. series.json → SeriesStore（逐 measurement）
//  3. shards.json → ShardIndex（逐 measurement）
//  4. schema.json → Schema（逐 measurement）
func (m *Manager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. 加载 manifest.json
	if err := m.catalog.loadLocked(); err != nil {
		return fmt.Errorf("load catalog: %w", err)
	}

	// 如果 manifest 为空，尝试从旧格式迁移
	if len(m.catalog.manifest.Databases) == 0 {
		m.mu.Unlock()
		_ = m.migrateFromOldFormat()
		m.mu.Lock()
	}

	// 2-4. 遍历所有 database/measurement，加载各自的元数据
	for dbName, db := range m.catalog.manifest.Databases {
		for measName := range db.Measurements {
			if err := m.series.loadLocked(dbName, measName); err != nil {
				return fmt.Errorf("load series %s/%s: %w", dbName, measName, err)
			}
			if err := m.shardIndex.loadLocked(dbName, measName); err != nil {
				return fmt.Errorf("load shards %s/%s: %w", dbName, measName, err)
			}
			if err := m.loadSchemaLocked(dbName, measName); err != nil {
				return fmt.Errorf("load schema %s/%s: %w", dbName, measName, err)
			}
		}
	}

	m.dirty.Store(false)
	return nil
}

// Persist 持久化所有脏元数据到磁盘。
func (m *Manager) Persist() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.dirty.Load() {
		return nil
	}

	// 1. 持久化 manifest.json
	if err := m.catalog.persistLocked(); err != nil {
		return fmt.Errorf("persist catalog: %w", err)
	}

	// 2. 持久化 series.json（每个 measurement）
	for dbName, db := range m.catalog.manifest.Databases {
		for measName := range db.Measurements {
			if err := m.series.persistLocked(dbName, measName); err != nil {
				return fmt.Errorf("persist series %s/%s: %w", dbName, measName, err)
			}
		}
	}

	// 3. 持久化 shards.json（每个 measurement）
	for dbName, db := range m.catalog.manifest.Databases {
		for measName := range db.Measurements {
			if err := m.shardIndex.persistLocked(dbName, measName); err != nil {
				return fmt.Errorf("persist shards %s/%s: %w", dbName, measName, err)
			}
		}
	}

	// 4. 持久化 schema.json（每个 measurement）
	for dbName, db := range m.catalog.manifest.Databases {
		for measName := range db.Measurements {
			if err := m.persistSchemaLocked(dbName, measName); err != nil {
				return fmt.Errorf("persist schema %s/%s: %w", dbName, measName, err)
			}
		}
	}

	m.dirty.Store(false)
	return nil
}

// Close 关闭所有子系统。
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, dbMeta := range m.dbMeta {
		_ = dbMeta.Close()
	}
	m.dbMeta = nil
	return nil
}

// getOrCreateDBMetaLocked 获取或创建 DatabaseMetaStore（需持有 m.mu）。
func (m *Manager) getOrCreateDBMetaLocked(db string) *measurement.DatabaseMetaStore {
	if dbMeta, ok := m.dbMeta[db]; ok {
		return dbMeta
	}
	dbMeta := measurement.NewDatabaseMetaStore()
	m.dbMeta[db] = dbMeta
	return dbMeta
}

// loadSchemaLocked 从磁盘加载 schema（已持有锁）。
func (m *Manager) loadSchemaLocked(db, meas string) error {
	schemaPath := filepath.Join(m.metaDir, db, meas, "schema.json")
	var s Schema
	if err := atomicRead(schemaPath, &s); err != nil {
		return err
	}
	if s.Version > 0 {
		m.schema[db+"/"+meas] = &s
	}
	return nil
}

// persistSchemaLocked 持久化 schema（已持有锁）。
func (m *Manager) persistSchemaLocked(db, meas string) error {
	s, ok := m.schema[db+"/"+meas]
	if !ok || s == nil {
		return nil
	}
	schemaPath := filepath.Join(m.metaDir, db, meas, "schema.json")
	return atomicWrite(schemaPath, s)
}
