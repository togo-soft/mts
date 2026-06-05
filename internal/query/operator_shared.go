package query

import "codeberg.org/micro-ts/mts/types"

// compareFieldValue 比较两个 FieldValue，返回 -1/0/1。
func compareFieldValue(a, b *types.FieldValue) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 快速路径: float64 vs float64（覆盖 >90% 比较，跳过完整 type switch）
	if af, ok := a.Value.(*types.FieldValue_FloatValue); ok {
		if bf, ok := b.Value.(*types.FieldValue_FloatValue); ok {
			if af.FloatValue < bf.FloatValue {
				return -1
			}
			if af.FloatValue > bf.FloatValue {
				return 1
			}
			return 0
		}
	}

	switch v := a.Value.(type) {
	case *types.FieldValue_IntValue:
		switch w := b.Value.(type) {
		case *types.FieldValue_IntValue:
			if v.IntValue < w.IntValue {
				return -1
			}
			if v.IntValue > w.IntValue {
				return 1
			}
			return 0
		case *types.FieldValue_FloatValue:
			fv := float64(v.IntValue)
			if fv < w.FloatValue {
				return -1
			}
			if fv > w.FloatValue {
				return 1
			}
			return 0
		}
	case *types.FieldValue_FloatValue:
		switch w := b.Value.(type) {
		case *types.FieldValue_FloatValue:
			if v.FloatValue < w.FloatValue {
				return -1
			}
			if v.FloatValue > w.FloatValue {
				return 1
			}
			return 0
		case *types.FieldValue_IntValue:
			fw := float64(w.IntValue)
			if v.FloatValue < fw {
				return -1
			}
			if v.FloatValue > fw {
				return 1
			}
			return 0
		}
	case *types.FieldValue_StringValue:
		switch w := b.Value.(type) {
		case *types.FieldValue_StringValue:
			if v.StringValue < w.StringValue {
				return -1
			}
			if v.StringValue > w.StringValue {
				return 1
			}
			return 0
		}
	case *types.FieldValue_BoolValue:
		switch w := b.Value.(type) {
		case *types.FieldValue_BoolValue:
			if !v.BoolValue && w.BoolValue {
				return -1
			}
			if v.BoolValue && !w.BoolValue {
				return 1
			}
			return 0
		}
	}
	return 0
}

// fieldValueFloat 从 FieldValue 提取 float64 值（支持 float64 和 int64）。
func fieldValueFloat(fv *types.FieldValue) (float64, bool) {
	if fv == nil {
		return 0, false
	}
	switch v := fv.Value.(type) {
	case *types.FieldValue_FloatValue:
		return v.FloatValue, true
	case *types.FieldValue_IntValue:
		return float64(v.IntValue), true
	}
	return 0, false
}
