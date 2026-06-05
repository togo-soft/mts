package query

import (
	"context"

	"codeberg.org/micro-ts/mts/types"
)

// FilterOperator 按条件过滤行，多个条件为 AND 关系。
type FilterOperator struct {
	upstream   Operator
	conditions []*types.FilterCondition
}

// NewFilterOperator 创建过滤算子。
func NewFilterOperator(upstream Operator, specs []*types.FilterCondition) *FilterOperator {
	return &FilterOperator{upstream: upstream, conditions: specs}
}

// Open 初始化上游算子。
func (f *FilterOperator) Open(ctx context.Context) error {
	return f.upstream.Open(ctx)
}

// Next 返回第一行通过所有条件的行。
func (f *FilterOperator) Next() (*types.PointRow, error) {
	for {
		row, err := f.upstream.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if f.matchAll(row) {
			return row, nil
		}
	}
}

// matchAll 检查行是否满足所有条件。
func (f *FilterOperator) matchAll(row *types.PointRow) bool {
	fieldIndex := make(map[string]*types.FieldValue, len(row.Fields))
	for _, fe := range row.Fields {
		fieldIndex[fe.Key] = fe.Value
	}
	for _, cond := range f.conditions {
		if !f.matchOne(row, cond, fieldIndex) {
			return false
		}
	}
	return true
}

// matchOne 检查行是否满足单个条件。
func (f *FilterOperator) matchOne(row *types.PointRow, cond *types.FilterCondition, fieldIndex map[string]*types.FieldValue) bool {
	if cond.Tag != "" {
		return f.matchTag(row, cond)
	}
	return f.matchField(cond, fieldIndex)
}

// matchTag 检查 tag 值是否满足条件。
func (f *FilterOperator) matchTag(row *types.PointRow, cond *types.FilterCondition) bool {
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

// matchField 检查字段值是否满足比较条件。
func (f *FilterOperator) matchField(cond *types.FilterCondition, fieldIndex map[string]*types.FieldValue) bool {
	fieldVal := fieldIndex[cond.Field]
	if fieldVal == nil {
		return false
	}

	switch cond.Op {
	case types.FilterOp_EQ:
		return compareFieldValue(fieldVal, cond.Value) == 0
	case types.FilterOp_NE:
		return compareFieldValue(fieldVal, cond.Value) != 0
	case types.FilterOp_GT:
		return compareFieldValue(fieldVal, cond.Value) > 0
	case types.FilterOp_GTE:
		return compareFieldValue(fieldVal, cond.Value) >= 0
	case types.FilterOp_LT:
		return compareFieldValue(fieldVal, cond.Value) < 0
	case types.FilterOp_LTE:
		return compareFieldValue(fieldVal, cond.Value) <= 0
	default:
		return false
	}
}

// Close 关闭上游算子。
func (f *FilterOperator) Close() error {
	return f.upstream.Close()
}
