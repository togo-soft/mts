// internal/types/types.go
package types

import (
	"time"
)

// Point 是写入的基本单位
type Point struct {
	Database    string
	Measurement string
	Tags        map[string]string
	Timestamp   int64 // 纳秒
	Fields      map[string]any
}

// PointRow 是查询结果的一行
type PointRow struct {
	SID       uint64            // Series ID，新增
	Timestamp int64
	Tags      map[string]string
	Fields    map[string]any
}

// QueryRangeRequest 定义范围查询
type QueryRangeRequest struct {
	Database    string
	Measurement string
	StartTime   int64
	EndTime     int64
	Fields      []string
	Tags        map[string]string
	Offset      int64
	Limit       int64
}

// QueryRangeResponse 返回查询结果
type QueryRangeResponse struct {
	Database    string
	Measurement string
	StartTime   int64
	EndTime     int64
	TotalCount  int64
	HasMore     bool
	Rows        []PointRow
}

// Config 配置
type Config struct {
	// Shard 时间窗口，最小 1 小时
	ShardDuration time.Duration

	// MemTable 刷盘阈值（最小 1MB）
	MemTableSize int64

	// WAL 刷盘策略
	WALSyncPolicy string

	// 数据目录
	DataDir string
}

// FieldType 字段类型
type FieldType int

const (
	FieldTypeInt64 FieldType = iota
	FieldTypeFloat64
	FieldTypeString
	FieldTypeBool
)

// FieldDef 字段定义
type FieldDef struct {
	Name string
	Type FieldType
}

// MeasurementMeta Measurement 元信息
type MeasurementMeta struct {
	Version     int64
	FieldSchema []FieldDef
	TagKeys     []string
	NextSID     int64
}
