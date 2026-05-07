package engine

import (
	"fmt"
)

// ListDatabases 列出所有数据库名称。
func (e *Engine) ListDatabases() []string {
	return e.manager.Catalog().ListDatabases()
}

// ListMeasurements 列出指定数据库中的所有 Measurement 名称。
func (e *Engine) ListMeasurements(database string) ([]string, bool) {
	names, err := e.manager.Catalog().ListMeasurements(database)
	if err != nil {
		return nil, false
	}
	return names, true
}

// CreateDatabase 创建一个新的数据库。
func (e *Engine) CreateDatabase(database string) bool {
	if e.manager.Catalog().DatabaseExists(database) {
		return false
	}
	_ = e.manager.Catalog().CreateDatabase(database)
	return true
}

// DropDatabase 删除指定的数据库。
func (e *Engine) DropDatabase(database string) bool {
	return e.manager.Catalog().DropDatabase(database) == nil
}

// CreateMeasurement 在指定数据库中创建一个新的 Measurement。
func (e *Engine) CreateMeasurement(database, measurement string) (bool, error) {
	if database == "" {
		return false, ErrEmptyDatabase
	}
	if measurement == "" {
		return false, ErrEmptyMeasurement
	}

	if err := e.manager.Catalog().CreateMeasurement(database, measurement); err != nil {
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

	if !e.manager.Catalog().DatabaseExists(database) {
		return false, fmt.Errorf("%w: %s", ErrDatabaseNotFound, database)
	}

	if !e.manager.Catalog().MeasurementExists(database, measurement) {
		return false, nil
	}

	err := e.manager.Catalog().DropMeasurement(database, measurement)
	if err != nil {
		return false, err
	}
	return true, nil
}
