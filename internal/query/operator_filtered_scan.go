package query

import (
	"context"

	"codeberg.org/micro-ts/mts/types"
)

// buildFieldIndex 构建 fieldName→Fields切片索引 的映射，实现 O(1) 字段查找。
func buildFieldIndex(fields []*types.FieldEntry) map[string]int {
	idx := make(map[string]int, len(fields))
	for i, f := range fields {
		idx[f.Key] = i
	}
	return idx
}

// matchFilterCondition 检查单行是否满足单个过滤条件（O(1) 字段查找）。
func matchFilterCondition(row *types.PointRow, cond *types.FilterCondition, fieldIndex map[string]int) bool {
	if cond.Tag != "" {
		val, ok := row.Tags[cond.Tag]
		if !ok {
			return false
		}
		switch cond.Op {
		case types.FilterOp_EQ:
			return val == cond.Value.GetStringValue()
		case types.FilterOp_NE:
			return val != cond.Value.GetStringValue()
		default:
			return false
		}
	}
	idx, ok := fieldIndex[cond.Field]
	if !ok {
		return false
	}
	return matchFieldOp(row.Fields[idx].Value, cond.Value, cond.Op)
}

// matchFieldOp 比较字段值是否满足运算条件。
func matchFieldOp(fieldVal, condVal *types.FieldValue, op types.FilterOp) bool {
	switch op {
	case types.FilterOp_EQ:
		return compareFieldValue(fieldVal, condVal) == 0
	case types.FilterOp_NE:
		return compareFieldValue(fieldVal, condVal) != 0
	case types.FilterOp_GT:
		return compareFieldValue(fieldVal, condVal) > 0
	case types.FilterOp_GTE:
		return compareFieldValue(fieldVal, condVal) >= 0
	case types.FilterOp_LT:
		return compareFieldValue(fieldVal, condVal) < 0
	case types.FilterOp_LTE:
		return compareFieldValue(fieldVal, condVal) <= 0
	default:
		return false
	}
}

// FilteredScanOperator 将扫描和字段过滤融合为单一算子，消除 Scan→Filter 之间的 interface 分发开销。
type FilteredScanOperator struct {
	iter       *Iterator
	conditions []*types.FilterCondition
	fieldIndex map[string]int
}

// NewFilteredScanOperator 创建融合的扫描+过滤算子。
func NewFilteredScanOperator(iter *Iterator, conditions []*types.FilterCondition) *FilteredScanOperator {
	return &FilteredScanOperator{iter: iter, conditions: conditions}
}

// Open 无需额外初始化。
func (f *FilteredScanOperator) Open(_ context.Context) error {
	return nil
}

// Next 直接返回通过所有过滤条件的下一行。
func (f *FilteredScanOperator) Next() (*types.PointRow, error) {
	for f.iter.Next(bgCtx) {
		row := f.iter.Points()
		if f.fieldIndex == nil {
			f.fieldIndex = buildFieldIndex(row.Fields)
		}
		for _, cond := range f.conditions {
			if !matchFilterCondition(row, cond, f.fieldIndex) {
				goto nextRow
			}
		}
		return row, nil
	nextRow:
	}
	return nil, nil
}

// Close 释放底层 Iterator 资源。
func (f *FilteredScanOperator) Close() error {
	return f.iter.Close()
}
