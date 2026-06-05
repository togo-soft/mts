package sstable

import "codeberg.org/micro-ts/mts/types"

// detectFieldType 检测字段类型。
func detectFieldType(val any) FieldType {
	if val == nil {
		return FieldTypeFloat64
	}

	if fv, ok := val.(*types.FieldValue); ok {
		if fv == nil || fv.Value == nil {
			return FieldTypeFloat64
		}
		switch fv.Value.(type) {
		case *types.FieldValue_FloatValue:
			return FieldTypeFloat64
		case *types.FieldValue_IntValue:
			return FieldTypeInt64
		case *types.FieldValue_StringValue:
			return FieldTypeString
		case *types.FieldValue_BoolValue:
			return FieldTypeBool
		}
		return FieldTypeFloat64
	}

	switch val.(type) {
	case float64:
		return FieldTypeFloat64
	case int64:
		return FieldTypeInt64
	case string:
		return FieldTypeString
	case bool:
		return FieldTypeBool
	}
	return FieldTypeFloat64
}
