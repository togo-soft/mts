// Package types 提供所有数据类型的定义和转换工具。
//
// 本包包含：
//   - protobuf 生成的消息类型（来自 microts.proto）
//   - 类型转换工具函数
//
// 类型分类：
//
//	基础类型：Point, PointRow, FieldValue, FieldType
//	gRPC 消息：WriteRequest, WriteResponse, QueryRangeRequest, etc.
//	配置类型：Config, MemTableConfig
//	元数据类型：MeasurementMeta, FieldDef
//
// 时间单位：
//
//	所有时间戳和时间间隔都使用纳秒（int64）。
//	转换辅助函数提供帮助从 time.Duration 转换。
package types

import "time"

// ===================================
// MemTableConfig 辅助函数
// ===================================

// GetIdleDuration 返回 IdleDurationNanos 的 time.Duration 形式。
func (c *MemTableConfig) GetIdleDuration() time.Duration {
	return time.Duration(c.IdleDurationNanos)
}

// SetIdleDuration 设置 IdleDurationNanos。
func (c *MemTableConfig) SetIdleDuration(d time.Duration) {
	c.IdleDurationNanos = int64(d)
}

// DefaultMemTableConfig 返回默认的 MemTableConfig。
//
// 默认配置：
//
//   - MaxSize: 64MB
//   - MaxCount: 3000
//   - IdleDuration: 1分钟
func DefaultMemTableConfig() *MemTableConfig {
	return &MemTableConfig{
		MaxSize:           64 * 1024 * 1024,
		MaxCount:          3000,
		IdleDurationNanos: int64(time.Minute),
	}
}

// ===================================
// Config 辅助函数
// ===================================

// GetShardDuration 返回 ShardDurationNanos 的 time.Duration 形式。
func (c *Config) GetShardDuration() time.Duration {
	return time.Duration(c.ShardDurationNanos)
}

// SetShardDuration 设置 ShardDurationNanos。
func (c *Config) SetShardDuration(d time.Duration) {
	c.ShardDurationNanos = int64(d)
}

// ===================================
// FieldValue 辅助函数
// ===================================

// NewFieldValue 从任何值创建 FieldValue。
//
// 支持的类型：int64, float64, string, bool。
// 如果类型不支持，返回 nil。
func NewFieldValue(v any) *FieldValue {
	switch val := v.(type) {
	case int64:
		return &FieldValue{Value: &FieldValue_IntValue{IntValue: val}}
	case float64:
		return &FieldValue{Value: &FieldValue_FloatValue{FloatValue: val}}
	case string:
		return &FieldValue{Value: &FieldValue_StringValue{StringValue: val}}
	case bool:
		return &FieldValue{Value: &FieldValue_BoolValue{BoolValue: val}}
	default:
		return nil
	}
}

// ===================================
// Point 辅助函数
// ===================================

// SetField 设置 Point 的字段值。
func (p *Point) SetField(name string, value any) {
	if p.Fields == nil {
		p.Fields = make(map[string]*FieldValue)
	}
	p.Fields[name] = NewFieldValue(value)
}

// GetField 获取 Point 的字段值。
func (p *Point) GetField(name string) any {
	if p.Fields == nil {
		return nil
	}
	if fv, ok := p.Fields[name]; ok && fv != nil {
		return fv.GetValue()
	}
	return nil
}

// ===================================
// PointRow 辅助函数
// ===================================

// SetField 设置 PointRow 的字段值。
func (p *PointRow) SetField(name string, value any) {
	if p.Fields == nil {
		p.Fields = make(map[string]*FieldValue)
	}
	p.Fields[name] = NewFieldValue(value)
}

// GetField 获取 PointRow 的字段值。
func (p *PointRow) GetField(name string) any {
	if p.Fields == nil {
		return nil
	}
	if fv, ok := p.Fields[name]; ok && fv != nil {
		return fv.GetValue()
	}
	return nil
}

// ToPoint 将 PointRow 转换为 Point。
//
// 注意：SID 不会被复制到 Point（Point 没有 SID 字段）。
func (p *PointRow) ToPoint(database, measurement string) *Point {
	if p == nil {
		return nil
	}
	return &Point{
		Database:    database,
		Measurement: measurement,
		Tags:        p.Tags,
		Timestamp:   p.Timestamp,
		Fields:      p.Fields,
	}
}
