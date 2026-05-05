// Package types 提供配置相关的类型定义。
//
// 配置类型用于初始化数据库引擎，控制存储行为和资源使用。
// 大多数配置都有合理的默认值，零值表示使用默认设置。
package types

import "time"

// FieldType 定义字段的数据类型。
//
// 支持的类型：
//
//	FieldTypeUnknown:  未知/无效类型
//	FieldTypeInt64:    64位有符号整数
//	FieldTypeFloat64:  64位浮点数
//	FieldTypeString:   可变长度字符串
//	FieldTypeBool:     布尔值
//
// 自动类型检测：
//
//	写入时如果字段类型未指定，数据库会尝试根据值自动推断。
//	推断规则：float64 → int64 → string → bool（优先级递减）
type FieldType int

const (
	FieldTypeUnknown FieldType = iota // 0 = 未知/无效类型
	FieldTypeInt64
	FieldTypeFloat64
	FieldTypeString
	FieldTypeBool
)

// FieldDef 定义字段的元信息。
//
// 包含字段名称和数据类型，用于 Schema 定义和类型检查。
//
// 使用场景：
//
//	内部使用，用于存储和维护 Measurement 的字段 Schema。
//	用户通常不需要直接使用，类型由第一次写入自动推断。
type FieldDef struct {
	Name string
	Type FieldType
}

// MeasurementMeta 包含 Measurement 的元信息。
//
// 维护了字段的 Schema、标签键列表和下一个 Series ID。
// 这部分信息通过 MetaStore 接口进行持久化。
//
// 字段说明：
//
//   - Version:     Schema 版本号，用于 Schema 演进
//   - FieldSchema: 字段定义列表
//   - TagKeys:     所有标签的键名
//   - NextSID:     下一个待分配的 Series ID
//
// 内部使用：
//
//	用户不直接操作此结构，由数据库内部管理。
//	可以通过元数据 API 查询这些信息。
type MeasurementMeta struct {
	Version     int64
	FieldSchema []FieldDef
	TagKeys     []string
	NextSID     int64
}

// Config 是数据库的内部配置结构。
//
// 这是存储引擎使用的完整配置，用户在公共 API 中使用更简单的 Config。
//
// 字段说明：
//
//   - DataDir:       数据存储目录路径
//   - ShardDuration: Shard 时间窗口，最小 1 小时
//   - MemTableCfg:   MemTable 配置
//
// ShardDuration 说明：
//
//	ShardDuration 控制数据在磁盘上的分片大小。
//	较小的值会产生更多小文件，利于过期数据快速清理。
//	较大的值减少文件数量，提高大时间范围查询效率。
//	建议值：1小时 - 7天。
//
// 内部使用：
//
//	此配置由引擎使用，公共 API 提供简化的 Config。
type Config struct {
	// 数据目录
	DataDir string

	// Shard 时间窗口，最小 1 小时
	ShardDuration time.Duration

	// MemTable 配置
	MemTableCfg MemTableConfig
}

// MemTableConfig 配置内存表的行为。
//
// 内存表（MemTable）写入缓冲区，数据先写入 MemTable，
// 当满足任一条件时刷新到 SSTable。
//
// 刷新条件（满足任一即触发刷新）：
//
//   - MaxSize:      内存占用（估算）达到上限
//   - MaxCount:     条目数达到上限
//   - IdleDuration: 超过空闲时间没有新数据写入
//
// 性能调优建议：
//
//	增加 MaxSize 可以减少刷盘频率，但会增加内存占用和恢复时间。
//	减少 IdleDuration 可以更快释放内存，但会增加小文件数量。
//	MaxCount 主要作为后备保护。
//
// 默认值（由 DefaultMemTableConfig 提供）：
//
//   - MaxSize: 64MB
//   - MaxCount: 3000
//   - IdleDuration: 1分钟
//
// 使用示例：
//
//	cfg := types.MemTableConfig{
//	    MaxSize:      256 * 1024 * 1024, // 256MB，适合内存充足的场景
//	    MaxCount:     100000,
//	    IdleDuration: 5 * time.Minute,   // 5分钟，适合低频率写入
//	}
type MemTableConfig struct {
	// MaxSize 最大内存大小（字节），默认 64MB
	MaxSize int64

	// MaxCount 最大条目数，默认 3000
	MaxCount int

	// IdleDuration 空闲时间阈值，数据持续该时间没有写入则触发 flush
	IdleDuration time.Duration
}
