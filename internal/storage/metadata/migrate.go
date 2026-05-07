package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// migrateFromOldFormat 从旧目录布局迁移到 _metadata 结构。
//
// 迁移内容：
//  1. 扫描 {dataDir} 目录，发现所有 database/measurement
//  2. 从旧 meta.json 迁移 series 数据到 series.json
//  3. 构建 manifest.json（从已发现的目录结构）
func (m *Manager) migrateFromOldFormat() error {
	entries, err := os.ReadDir(m.dataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dbName := entry.Name()
		if dbName == "_metadata" {
			continue
		}

		measEntries, err := os.ReadDir(filepath.Join(m.dataDir, dbName))
		if err != nil {
			return fmt.Errorf("read dir %s: %w", dbName, err)
		}

		for _, measEntry := range measEntries {
			if !measEntry.IsDir() {
				continue
			}
			measName := measEntry.Name()

			_ = m.catalog.CreateDatabase(dbName)
			_ = m.catalog.CreateMeasurement(dbName, measName)
			_ = m.migrateSeriesLocked(dbName, measName)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UnixNano()
	for dbName, db := range m.catalog.manifest.Databases {
		if db.CreatedAt == 0 {
			db.CreatedAt = now
			m.catalog.manifest.Databases[dbName] = db
		}
		for measName, meas := range db.Measurements {
			if meas.CreatedAt == 0 {
				meas.CreatedAt = now
				db.Measurements[measName] = meas
			}
		}
	}

	return nil
}

// migrateSeriesLocked 迁移单个 measurement 的 series 数据。
func (m *Manager) migrateSeriesLocked(db, meas string) error {
	oldPath := filepath.Join(m.dataDir, db, meas, "meta.json")
	if _, err := os.Stat(oldPath); os.IsNotExist(err) {
		return nil
	}

	return m.series.loadLocked(db, meas)
}
