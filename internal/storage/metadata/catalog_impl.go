package metadata

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"
)

// ===================================
// shimCatalog — Catalog 的包装实现
// ===================================

type shimCatalog struct {
	m        *Manager
	manifest manifestData
}

func newShimCatalog(m *Manager) *shimCatalog {
	return &shimCatalog{
		m: m,
		manifest: manifestData{
			Version:   1,
			Databases: make(map[string]dbManifest),
		},
	}
}

func (c *shimCatalog) loadLocked() error {
	return atomicRead(filepath.Join(c.m.metaDir, "manifest.json"), &c.manifest)
}

func (c *shimCatalog) persistLocked() error {
	return atomicWrite(filepath.Join(c.m.metaDir, "manifest.json"), &c.manifest)
}

func (c *shimCatalog) ensureDBLocked(name string) *dbManifest {
	if db, ok := c.manifest.Databases[name]; ok {
		return &db
	}
	db := dbManifest{
		Measurements: make(map[string]measManifest),
		CreatedAt:    time.Now().UnixNano(),
	}
	c.manifest.Databases[name] = db
	c.m.markDirty()
	return &db
}

func (c *shimCatalog) CreateDatabase(name string) error {
	if name == "" {
		return fmt.Errorf("database name is empty")
	}

	c.m.mu.Lock()
	defer c.m.mu.Unlock()

	if _, ok := c.manifest.Databases[name]; ok {
		return nil
	}
	c.manifest.Databases[name] = dbManifest{
		Measurements: make(map[string]measManifest),
		CreatedAt:    time.Now().UnixNano(),
	}
	c.m.getOrCreateDBMetaLocked(name)
	c.m.markDirty()
	return nil
}

func (c *shimCatalog) DropDatabase(name string) error {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()

	dbMeta, ok := c.m.dbMeta[name]
	if !ok {
		return fmt.Errorf("database %q not found", name)
	}
	_ = dbMeta.Close()
	delete(c.m.dbMeta, name)
	delete(c.manifest.Databases, name)
	c.m.markDirty()
	return nil
}

func (c *shimCatalog) ListDatabases() []string {
	c.m.mu.RLock()
	defer c.m.mu.RUnlock()

	names := make([]string, 0, len(c.manifest.Databases))
	for name := range c.manifest.Databases {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *shimCatalog) DatabaseExists(name string) bool {
	c.m.mu.RLock()
	defer c.m.mu.RUnlock()
	_, ok := c.manifest.Databases[name]
	return ok
}

func (c *shimCatalog) CreateMeasurement(database, name string) error {
	if database == "" {
		return fmt.Errorf("database name is empty")
	}
	if name == "" {
		return fmt.Errorf("measurement name is empty")
	}

	c.m.mu.Lock()
	defer c.m.mu.Unlock()

	_ = c.m.getOrCreateDBMetaLocked(database)
	db := c.ensureDBLocked(database)
	if _, ok := db.Measurements[name]; ok {
		return nil
	}
	db.Measurements[name] = measManifest{
		SchemaVersion: 0,
		CreatedAt:     time.Now().UnixNano(),
	}
	c.m.getOrCreateDBMetaLocked(database).GetOrCreate(name)
	c.m.markDirty()
	return nil
}

func (c *shimCatalog) DropMeasurement(database, name string) error {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()

	if _, ok := c.manifest.Databases[database]; !ok {
		return fmt.Errorf("database %q not found", database)
	}

	dbMeta := c.m.dbMeta[database]
	if !dbMeta.DropMeasurement(name) {
		return fmt.Errorf("measurement %q not found in database %q", name, database)
	}
	delete(c.manifest.Databases[database].Measurements, name)
	c.m.markDirty()
	return nil
}

func (c *shimCatalog) ListMeasurements(database string) ([]string, error) {
	c.m.mu.RLock()
	defer c.m.mu.RUnlock()

	dbMeta, ok := c.m.dbMeta[database]
	if !ok {
		return nil, fmt.Errorf("database %q not found", database)
	}
	return dbMeta.ListMeasurements(), nil
}

func (c *shimCatalog) MeasurementExists(database, name string) bool {
	c.m.mu.RLock()
	defer c.m.mu.RUnlock()

	dbMeta, ok := c.m.dbMeta[database]
	if !ok {
		return false
	}
	return dbMeta.MeasurementExists(name)
}

func (c *shimCatalog) GetRetention(database, measurement string) (time.Duration, error) {
	c.m.mu.RLock()
	defer c.m.mu.RUnlock()

	db, ok := c.manifest.Databases[database]
	if !ok {
		return 0, fmt.Errorf("database %q not found", database)
	}
	meas, ok := db.Measurements[measurement]
	if !ok {
		return 0, fmt.Errorf("measurement %q not found", measurement)
	}
	return time.Duration(meas.RetentionNs), nil
}

func (c *shimCatalog) SetRetention(database, measurement string, d time.Duration) error {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()

	db, ok := c.manifest.Databases[database]
	if !ok {
		return fmt.Errorf("database %q not found", database)
	}
	meas, ok := db.Measurements[measurement]
	if !ok {
		return fmt.Errorf("measurement %q not found", measurement)
	}
	meas.RetentionNs = int64(d)
	db.Measurements[measurement] = meas
	c.m.markDirty()
	return nil
}

func (c *shimCatalog) GetSchema(database, measurement string) (*Schema, error) {
	c.m.mu.RLock()
	defer c.m.mu.RUnlock()

	db, ok := c.manifest.Databases[database]
	if !ok {
		return nil, fmt.Errorf("database %q not found", database)
	}
	if _, ok := db.Measurements[measurement]; !ok {
		return nil, fmt.Errorf("measurement %q not found", measurement)
	}
	return c.m.schema[database+"/"+measurement], nil
}

func (c *shimCatalog) SetSchema(database, measurement string, s *Schema) error {
	if s == nil {
		return fmt.Errorf("schema is nil")
	}

	c.m.mu.Lock()
	defer c.m.mu.Unlock()

	db, ok := c.manifest.Databases[database]
	if !ok {
		return fmt.Errorf("database %q not found", database)
	}
	meas, ok := db.Measurements[measurement]
	if !ok {
		return fmt.Errorf("measurement %q not found", measurement)
	}

	existing := c.m.schema[database+"/"+measurement]
	if existing != nil {
		if err := validateSchemaUpdate(existing, s); err != nil {
			return err
		}
	}

	s.UpdatedAt = time.Now().UnixNano()
	c.m.schema[database+"/"+measurement] = s

	meas.SchemaVersion++
	db.Measurements[measurement] = meas
	c.m.markDirty()
	return nil
}

// validateSchemaUpdate 校验 schema 更新是否兼容。
func validateSchemaUpdate(old, new *Schema) error {
	for _, oldField := range old.Fields {
		for _, newField := range new.Fields {
			if oldField.Name == newField.Name && oldField.Type != newField.Type {
				return fmt.Errorf("incompatible field type change for %q: %d -> %d",
					oldField.Name, oldField.Type, newField.Type)
			}
		}
	}
	return nil
}
