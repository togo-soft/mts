package engine

import (
	"fmt"
	"log/slog"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

// ListDatabases 列出所有数据库名称。
func (e *Engine) ListDatabases() []string {
	return e.catalog.ListDatabases()
}

// ListMeasurements 列出指定数据库中的所有 Measurement 名称。
func (e *Engine) ListMeasurements(database string) ([]string, bool) {
	names, err := e.catalog.ListMeasurements(database)
	if err != nil {
		return nil, false
	}
	return names, true
}

// CreateDatabase 创建一个新的数据库。
//
// retention 为数据保留时间，为 0 时使用全局默认值或不启用过期清理。
// downsample 为降采样配置，为 nil 时不启用降采样。
func (e *Engine) CreateDatabase(database string, retention time.Duration, downsample *types.DownsampleConfig) bool {
	if e.catalog.DatabaseExists(database) {
		return false
	}
	if err := e.catalog.CreateDatabase(database); err != nil {
		slog.Warn("failed to create database", "database", database, "error", err)
		return false
	}
	if retention > 0 {
		if err := e.catalog.SetDatabaseRetention(database, retention); err != nil {
			slog.Warn("failed to set database retention", "database", database, "error", err)
		}
	}
	if downsample != nil && downsample.Enabled {
		if err := e.catalog.SetDownsampleConfig(database, downsample); err != nil {
			slog.Warn("failed to set downsample config", "database", database, "error", err)
		}
	}
	return true
}

// DropDatabase 删除指定的数据库。
func (e *Engine) DropDatabase(database string) bool {
	return e.catalog.DropDatabase(database) == nil
}

// CreateMeasurement 在指定数据库中创建一个新的 Measurement。
func (e *Engine) CreateMeasurement(database, measurement string) (bool, error) {
	if database == "" {
		return false, ErrEmptyDatabase
	}
	if measurement == "" {
		return false, ErrEmptyMeasurement
	}

	if err := e.catalog.CreateMeasurement(database, measurement); err != nil {
		return false, err
	}
	return true, nil
}

// DropMeasurement 删除指定的 Measurement。
func (e *Engine) DropMeasurement(database, measurement string) (bool, error) {
	if database == "" {
		return false, ErrEmptyDatabase
	}
	if measurement == "" {
		return false, ErrEmptyMeasurement
	}

	if !e.catalog.DatabaseExists(database) {
		return false, fmt.Errorf("%w: %s", ErrDatabaseNotFound, database)
	}

	if !e.catalog.MeasurementExists(database, measurement) {
		return false, nil
	}

	err := e.catalog.DropMeasurement(database, measurement)
	if err != nil {
		return false, err
	}
	return true, nil
}
