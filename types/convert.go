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

// GetFlushIdle 返回 FlushIdleNanos 的 time.Duration 形式。
func (c *MemTableConfig) GetFlushIdle() time.Duration {
	return time.Duration(c.FlushIdleNanos)
}

// SetFlushIdle 设置 FlushIdleNanos。
func (c *MemTableConfig) SetFlushIdle(d time.Duration) {
	c.FlushIdleNanos = int64(d)
}

// DefaultMemTableConfig 返回默认的 MemTableConfig。
//
// 默认配置：
//
//   - FlushSize: 64MB
//   - FlushCount: 50000
//   - FlushIdle: 1分钟
func DefaultMemTableConfig() *MemTableConfig {
	return &MemTableConfig{
		FlushSize:      64 * 1024 * 1024,
		FlushCount:     50000,
		FlushIdleNanos: int64(time.Minute),
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
	case int:
		return &FieldValue{Value: &FieldValue_IntValue{IntValue: int64(val)}}
	case float64:
		return &FieldValue{Value: &FieldValue_FloatValue{FloatValue: val}}
	case float32:
		return &FieldValue{Value: &FieldValue_FloatValue{FloatValue: float64(val)}}
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

// SetField 设置 PointRow 的字段值（存在则替换，不存在则追加）。
func (p *PointRow) SetField(name string, value any) {
	fv := NewFieldValue(value)
	for i, f := range p.Fields {
		if f.Key == name {
			p.Fields[i] = &FieldEntry{Key: name, Value: fv}
			return
		}
	}
	p.Fields = append(p.Fields, &FieldEntry{Key: name, Value: fv})
}

// GetField 获取 PointRow 的字段值。
func (p *PointRow) GetField(name string) any {
	for _, f := range p.Fields {
		if f.Key == name && f.Value != nil {
			return f.Value.GetValue()
		}
	}
	return nil
}

// GetFieldValue 获取 PointRow 的字段值（返回 *FieldValue，方便链式调用 GetFloatValue 等）。
func (p *PointRow) GetFieldValue(name string) *FieldValue {
	for _, f := range p.Fields {
		if f.Key == name {
			return f.Value
		}
	}
	return nil
}

// GetFieldValue 获取 Row 的字段值（返回 *FieldValue）。
func (r *Row) GetFieldValue(name string) *FieldValue {
	for _, f := range r.Fields {
		if f.Key == name {
			return f.Value
		}
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
	// Point.Fields 仍然是 map，需要转换
	fields := make(map[string]*FieldValue, len(p.Fields))
	for _, f := range p.Fields {
		fields[f.Key] = f.Value
	}
	return &Point{
		Database:    database,
		Measurement: measurement,
		Tags:        p.Tags,
		Timestamp:   p.Timestamp,
		Fields:      fields,
	}
}
