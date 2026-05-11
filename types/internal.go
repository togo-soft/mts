package types

// InternalField 紧凑字段条目，避免每行分配 map。
type InternalField struct {
	Key   string
	Value *FieldValue
}

// InternalPoint 内部管线中的数据点，不含外部协议开销。
// 仅包含内部存储和查询所需的最小字段集。
type InternalPoint struct {
	Timestamp int64
	Fields    []InternalField
	Sid       uint64
}

// PointToInternal 将外部 Point 转换为 InternalPoint。
func PointToInternal(p *Point, sid uint64) InternalPoint {
	fields := make([]InternalField, 0, len(p.Fields))
	for k, v := range p.Fields {
		fields = append(fields, InternalField{Key: k, Value: v})
	}
	return InternalPoint{
		Timestamp: p.Timestamp,
		Fields:    fields,
		Sid:       sid,
	}
}

// InternalFieldsToMap 将 []InternalField 还原为 map[string]*FieldValue。
func InternalFieldsToMap(fields []InternalField) map[string]*FieldValue {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]*FieldValue, len(fields))
	for _, f := range fields {
		m[f.Key] = f.Value
	}
	return m
}

// MapToInternalFields 将 map[string]*FieldValue 转换为 []InternalField。
func MapToInternalFields(m map[string]*FieldValue) []InternalField {
	if len(m) == 0 {
		return nil
	}
	fields := make([]InternalField, 0, len(m))
	for k, v := range m {
		fields = append(fields, InternalField{Key: k, Value: v})
	}
	return fields
}
