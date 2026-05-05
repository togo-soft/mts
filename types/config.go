// Package types 配置相关类型
package types

import "time"

// FieldType 字段类型
type FieldType int

const (
	FieldTypeUnknown FieldType = iota // 0 = 未知/无效类型
	FieldTypeInt64
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

// Config 数据库配置（内部使用的完整配置）
type Config struct {
	// 数据目录
	DataDir string

	// Shard 时间窗口，最小 1 小时
	ShardDuration time.Duration

	// MemTable 配置
	MemTableCfg MemTableConfig
}

// MemTableConfig MemTable 配置
type MemTableConfig struct {
	// MaxSize 最大内存大小（字节），默认 64MB
	MaxSize int64

	// MaxCount 最大条目数，默认 3000
	MaxCount int

	// IdleDuration 空闲时间阈值，数据持续该时间没有写入则触发 flush
	IdleDuration time.Duration
}
