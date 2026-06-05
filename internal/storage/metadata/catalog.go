// Package metadata 提供全局元数据管理。
//
// Manager 是聚合入口，统一管理 Catalog、SeriesStore 和 ShardIndex 三个子系统。
package metadata

import (
	"time"

	"codeberg.org/micro-ts/mts/types"
)

// Schema 定义 Measurement 的字段和标签结构。
type Schema struct {
	Version   int64      `json:"version"`
	Fields    []FieldDef `json:"fields"`
	TagKeys   []string   `json:"tag_keys"`
	UpdatedAt int64      `json:"updated_at"`
}

// 字段类型 ID 常量（用于 FieldDef.Type）。
const (
	FieldTypeFloat64ID = 1
	FieldTypeInt64ID   = 2
	FieldTypeStringID  = 3
	FieldTypeBoolID    = 4
)

// FieldDef 定义单个字段的属性。
type FieldDef struct {
	Name     string `json:"name"`
	Type     int32  `json:"type"`
	Nullable bool   `json:"nullable,omitempty"`
	Default  string `json:"default,omitempty"`
}

// Catalog 管理 database 和 measurement 的目录结构。
type Catalog interface {
	CreateDatabase(name string) error
	DropDatabase(name string) error
	ListDatabases() []string
	DatabaseExists(name string) bool

	CreateMeasurement(database, name string) error
	DropMeasurement(database, name string) error
	ListMeasurements(database string) ([]string, error)
	MeasurementExists(database, name string) bool

	GetSchema(database, measurement string) (*Schema, error)
	SetSchema(database, measurement string, s *Schema) error

	GetRetention(database, measurement string) (time.Duration, error)
	SetRetention(database, measurement string, d time.Duration) error

	GetDatabaseRetention(database string) (time.Duration, error)
	SetDatabaseRetention(database string, d time.Duration) error

	GetDownsampleConfig(database string) (*types.DownsampleConfig, error)
	SetDownsampleConfig(database string, cfg *types.DownsampleConfig) error
}
